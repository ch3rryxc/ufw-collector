import socket
import ssl
import time
import os
import queue
import logging
import threading
import random
import signal

# ================= CONFIG =================
logFile = '/var/log/ufw.log'

serverHost = 'local.cloud.lan'
serverPort = 9050

clientId = socket.gethostname()

queueMaxSize = 20000
reconnectBaseDelay = 2
reconnectMaxDelay = 30

readSleep = 0.1

# ================= LOGGING =================
logging.basicConfig(
    level = logging.INFO,
    format = '[%(asctime)s] [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S'
)

# ================= STATE =================
bufferQueue = queue.Queue(maxsize = queueMaxSize)
shutdownEvent = threading.Event()

droppedMessages = 0

# ================= TLS =================
context = ssl.create_default_context()
context.load_verify_locations('server.crt')


# ================= NETWORK =================
def createConnection():
    sock = socket.create_connection((serverHost, serverPort), timeout = 10)

    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    tlsSock = context.wrap_socket(sock, server_hostname = serverHost)

    return tlsSock


def reconnectDelay(attempt):
    base = min(reconnectMaxDelay, reconnectBaseDelay * (2 ** attempt))
    jitter = random.uniform(0, base * 0.2)
    return base + jitter


# ================= FILE FOLLOW =================
def followFile(path):
    fileObj = open(path, 'r')
    inode = os.fstat(fileObj.fileno()).st_ino

    fileObj.seek(0, os.SEEK_END)

    logging.info('Start following log')

    while not shutdownEvent.is_set():
        line = fileObj.readline()

        if line:
            yield line.rstrip()
            continue

        # rotation handling
        try:
            stat = os.stat(path)

            if stat.st_ino != inode:
                logging.info('Log rotated')

                newFile = open(path, 'r')

                # дочитываем остаток старого файла
                for remaining in fileObj:
                    yield remaining.rstrip()

                fileObj.close()
                fileObj = newFile
                inode = os.fstat(fileObj.fileno()).st_ino

        except FileNotFoundError:
            pass

        time.sleep(readSleep)


# ================= READER =================
def readerLoop():
    global droppedMessages

    for line in followFile(logFile):
        if shutdownEvent.is_set():
            break

        message = f'{clientId} {line}'

        try:
            bufferQueue.put_nowait(message)
        except queue.Full:
            droppedMessages += 1

            if droppedMessages % 100 == 0:
                logging.warning(f'Dropped messages: {droppedMessages}')


# ================= SENDER =================
def senderLoop():
    attempt = 0
    tls = None

    while not shutdownEvent.is_set():
        if tls is None:
            try:
                tls = createConnection()
                logging.info('Connected to collector')
                attempt = 0
            except Exception as e:
                delay = reconnectDelay(attempt)
                logging.error(f'Connect failed: {e}, retry in {delay:.1f}s')
                time.sleep(delay)
                attempt += 1
                continue

        try:
            message = bufferQueue.get(timeout = 1)

            data = (message + '\n').encode()
            view = memoryview(data)

            while view:
                sent = tls.send(view)
                view = view[sent:]

        except queue.Empty:
            continue

        except Exception as e:
            logging.error(f'Connection lost: {e}')

            try:
                tls.close()
            except Exception:
                pass

            tls = None


# ================= METRICS =================
def metricsLoop():
    while not shutdownEvent.is_set():
        logging.info(
            f'Queue size={bufferQueue.qsize()} dropped={droppedMessages}'
        )
        time.sleep(10)


# ================= SHUTDOWN =================
def handleShutdown(signum, frame):
    logging.info('Shutdown signal received')
    shutdownEvent.set()


# ================= MAIN =================
def main():
    print(r'''   ______      _____________            __ 
  / ____/___  / / / ____/ (_)__  ____  / /_
 / /   / __ \/ / / /   / / / _ \/ __ \/ __/
/ /___/ /_/ / / / /___/ / /  __/ / / / /_  
\____/\____/_/_/\____/_/_/\___/_/ /_/\__/  
                                           ''')

    signal.signal(signal.SIGINT, handleShutdown)
    signal.signal(signal.SIGTERM, handleShutdown)

    threads = [
        threading.Thread(target = readerLoop, name = 'reader', daemon = True),
        threading.Thread(target = senderLoop, name = 'sender', daemon = True),
        threading.Thread(target = metricsLoop, name = 'metrics', daemon = True)
    ]

    for t in threads:
        t.start()

    try:
        while not shutdownEvent.is_set():
            time.sleep(1)
    finally:
        logging.info('Stopping...')

    for t in threads:
        t.join(timeout = 2)

    logging.info('Shutdown complete')


if __name__ == '__main__':
    main()
