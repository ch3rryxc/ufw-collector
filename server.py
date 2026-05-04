import re
from datetime import datetime
import socket
import ssl
import threading
import queue
import time
import logging
from contextlib import closing
from functools import lru_cache
import urllib.request
import os

from clickhouse_driver import Client
import geoip2.database

logging.basicConfig(
    level = logging.INFO,
    format = '[%(asctime)s] [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S'
)

# ================= REGEX =================
srcRegex = re.compile(r'SRC=([0-9.]+)')
dstRegex = re.compile(r'DST=([0-9.]+)')
protoRegex = re.compile(r'PROTO=([A-Z]+)')
sptRegex = re.compile(r'SPT=(\d+)')
dptRegex = re.compile(r'DPT=(\d+)')
inRegex = re.compile(r'IN=([\w\-]+)')
outRegex = re.compile(r'OUT=([\w\-]*)')

# ================= CONFIG =================
queueMaxSize = 50000
batchSize = 500
workerCount = 1
flushInterval = 1.0

eventQueue = queue.Queue(maxsize = queueMaxSize)
shutdownEvent = threading.Event()

clickhouseHost = '127.0.0.1'
clickhousePort = 9000  # FIXED

# ================= ENRICHMENT =================
geoReader = geoip2.database.Reader('data/GeoLite2-City.mmdb')
asnReader = geoip2.database.Reader('data/GeoLite2-ASN.mmdb')

@lru_cache(maxsize = 100000)
def enrich(ip):
    country = 'ZZ'
    lat = 0.0
    lon = 0.0
    asn = 0
    org = ''

    try:
        geo = geoReader.city(ip)
        if geo.country.iso_code:
            country = geo.country.iso_code
        if geo.location.latitude:
            lat = float(geo.location.latitude)
        if geo.location.longitude:
            lon = float(geo.location.longitude)
    except Exception as e:
        logging.debug(f'Geo error: {e}')

    try:
        a = asnReader.asn(ip)
        asn = a.autonomous_system_number or 0
        org = a.autonomous_system_organization or ''
    except Exception as e:
        logging.debug(f'ASN error: {e}')

    return country, lat, lon, asn, org

# ================= PARSER =================
def parseUfwLine(line):
    if 'UFW ' not in line:
        return None

    try:
        parts = line.split()
        timestampStr = parts[0]
        host = parts[1]

        timeObj = datetime.fromisoformat(timestampStr)
        timeUnix = int(timeObj.timestamp())

        def safeGet(regex):
            m = regex.search(line)
            return m.group(1) if m else None

        return {
            'time': timeUnix,
            'host': host,
            'action': 'BLOCK' if 'UFW BLOCK' in line else 'ALLOW',
            'src': safeGet(srcRegex),
            'dst': safeGet(dstRegex),
            'proto': safeGet(protoRegex),
            'srcPort': int(safeGet(sptRegex)) if safeGet(sptRegex) else None,
            'dstPort': int(safeGet(dptRegex)) if safeGet(dptRegex) else None,
            'inInterface': safeGet(inRegex),
            'outInterface': safeGet(outRegex)
        }

    except Exception as e:
        logging.debug(f'Parse error: {e}')
        return None

# ================= CLICKHOUSE =================
insertSql = '''
INSERT INTO logs (
    id, time, client, host, action,
    src, dst, proto,
    srcPort, dstPort,
    inInterface, outInterface,
    country, lat, lon,
    asn, org
) VALUES
'''
def generateId():
    return int(time.time() * 1_000_000) + threading.get_ident() % 1000

def buildRows(batch):
    rows = []

    for rec in batch:
        (
            clientHost,
            timeUnix,
            host,
            action,
            src,
            dst,
            proto,
            srcPort,
            dstPort,
            inIf,
            outIf
        ) = rec

        if src:
            country, lat, lon, asn, org = enrich(src)
        else:
            country, lat, lon, asn, org = ('ZZ', 0.0, 0.0, 0, '')

        rows.append((
            generateId(),
            timeUnix,
            clientHost,
            host,
            action or '',
            src or '0.0.0.0',
            dst or '0.0.0.0',
            proto or '',
            srcPort or 0,
            dstPort or 0,
            inIf or '',
            outIf or '',
            country,
            lat,
            lon,
            asn,
            org
        ))

    return rows

def insertWithRetry(client, rows, workerId):
    for attempt in range(3):
        try:
            client.execute(insertSql, rows)
            logging.info(f'Worker {workerId}: inserted {len(rows)}')
            return
        except Exception as e:
            logging.error(f'CH error (attempt {attempt+1}): {e}')
            time.sleep(0.5 * (attempt + 1))

def clickhouseWorker(workerId):
    client = Client(host = clickhouseHost, port = clickhousePort)

    logging.info(f'Worker {workerId} started')

    lastFlush = time.time()
    batch = []

    while not shutdownEvent.is_set() or not eventQueue.empty():
        try:
            item = eventQueue.get(timeout = 0.5)
            batch.append(item)
        except queue.Empty:
            pass

        now = time.time()

        if batch and (
            len(batch) >= batchSize or
            now - lastFlush >= flushInterval
        ):
            rows = buildRows(batch)
            insertWithRetry(client, rows, workerId)

            batch.clear()
            lastFlush = now

    if batch:
        rows = buildRows(batch)
        insertWithRetry(client, rows, workerId)
        logging.info(f'Worker {workerId}: final flush {len(rows)}')

    logging.info(f'Worker {workerId} stopped')

# ================= CLIENT HANDLER =================
def handleClient(conn, addr):
    threadName = f'client-{addr[0]}:{addr[1]}'
    threading.current_thread().name = threadName

    logging.info('Connected')

    try:
        with conn, conn.makefile('r') as fileObj:
            for line in fileObj:
                if shutdownEvent.is_set():
                    break

                line = line.strip()
                if not line:
                    continue

                parts = line.split(maxsplit = 1)
                if len(parts) != 2:
                    continue

                clientHost, raw = parts
                parsed = parseUfwLine(raw)

                if not parsed:
                    continue

                record = (
                    clientHost,
                    parsed['time'],
                    parsed['host'],
                    parsed['action'],
                    parsed['src'],
                    parsed['dst'],
                    parsed['proto'],
                    parsed['srcPort'],
                    parsed['dstPort'],
                    parsed['inInterface'],
                    parsed['outInterface']
                )

                try:
                    eventQueue.put(record, timeout = 0.2)
                except queue.Full:
                    logging.warning('Queue full — dropping event')

    except Exception as e:
        logging.error(f'Client error: {e}')

    finally:
        logging.info('Disconnected')

# ================= SERVER =================
def startServer(host, port):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(
        certfile = 'certs/server.crt',
        keyfile = 'certs/server.key'
    )

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        sock.bind((host, port))
        sock.listen(100)

        with context.wrap_socket(sock, server_side = True) as server:
            server.settimeout(1)

            logging.info(f'Listening on {host}:{port}')

            while not shutdownEvent.is_set():
                try:
                    conn, addr = server.accept()
                except socket.timeout:
                    continue
                except Exception as e:
                    logging.error(f'Accept error: {e}')
                    continue

                threading.Thread(
                    target = handleClient,
                    args = (conn, addr),
                    daemon = True
                ).start()

# ============== MMDB UPDATER ==============
def downloadFile(url, path):
    tmpPath = path + '.tmp'

    try:
        urllib.request.urlretrieve(url, tmpPath)

        if os.path.getsize(tmpPath) < 1000000:  
            raise Exception('Downloaded file too small')

        os.replace(tmpPath, path)  
        return True

    except Exception as e:
        logging.error(f'Download failed: {e}')
        try:
            os.remove(tmpPath)
        except:
            pass
        return False

def updateGeoDB():
    logging.info('Updating GeoIP DB...')

    cityPath = 'data/GeoLite2-City.mmdb'
    asnPath = 'data/GeoLite2-ASN.mmdb'

    ok1 = downloadFile('https://git.io/GeoLite2-City.mmdb', cityPath)
    ok2 = downloadFile('https://git.io/GeoLite2-ASN.mmdb', asnPath)

    if not (ok1 and ok2):
        logging.error('Geo update skipped')
        return

    newGeo = None
    newAsn = None

    try:
        newGeo = geoip2.database.Reader(cityPath)
        newAsn = geoip2.database.Reader(asnPath)

        global geoReader, asnReader

        oldGeo = geoReader
        oldAsn = asnReader

        geoReader = newGeo
        asnReader = newAsn

        def delayedClose(reader):
            try:
                reader.close()
            except Exception as e:
                logging.debug(f'Close error: {e}')

        t1 = threading.Timer(60, delayedClose, args = (oldGeo,))
        t1.daemon = True
        t1.start()

        t2 = threading.Timer(60, delayedClose, args = (oldAsn,))
        t2.daemon = True
        t2.start()

        enrich.cache_clear()

        logging.info('GeoIP updated successfully')

    except Exception as e:
        logging.error(f'Geo reload failed: {e}')

        if newGeo:
            try:
                newGeo.close()
            except:
                pass

        if newAsn:
            try:
                newAsn.close()
            except:
                pass

def geoUpdaterLoop():
    updateGeoDB()
    while not shutdownEvent.is_set():
        time.sleep(3600 * 12) 
        updateGeoDB()

# ================= MAIN =================
def main():
    host = '100.64.0.3'
    port = 9050

    # geo updater
    threading.Thread(
        target = geoUpdaterLoop,
        name = 'geo-updater',
        daemon = True
    ).start()

    # clickhouse workers
    for i in range(workerCount):
        threading.Thread(
            target = clickhouseWorker,
            args = (i,),
            name = f'ch-worker-{i}',
            daemon = True
        ).start()

    try:
        startServer(host, port)
    except KeyboardInterrupt:
        logging.info('Shutdown signal received')
        shutdownEvent.set()

    logging.info('Shutting down...')
    time.sleep(2)
    logging.info('Done')

if __name__ == '__main__':
    main()
