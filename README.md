# UFW Log Collector → ClickHouse

A lightweight, high-performance system for collecting, streaming, enriching, and storing **UFW firewall logs** in ClickHouse.

The project consists of two main components:

- **Collector (Server)** — receives logs over TLS, parses, enriches, and writes to ClickHouse  
- **Agent (Client)** — tails `/var/log/ufw.log` and streams events to the server  

---

## 🚀 Features

- 📡 Real-time log streaming  
- 🔒 Secure TLS connection (client → server)  
- 🌍 GeoIP + ASN enrichment (MaxMind GeoLite2)  
- ⚡ High-throughput batching with ClickHouse  
- 🧵 Multithreaded processing pipeline  
- 🔁 Automatic reconnection with backoff  
- 📦 Buffered queue with backpressure handling  
- 🔄 Automatic GeoIP database updates  

---

## 🏗 Architecture

```
+-------------+        TLS        +----------------+        +-------------+
|   Agent     |  ─────────────▶  |   Collector     | ─────▶ | ClickHouse  |
| (ufw.log)   |                  |                |        |             |
+-------------+                  +----------------+        +-------------+
       │                                │
       │                                ├─ Parsing (regex)
       │                                ├─ GeoIP enrichment
       │                                └─ Batch insert
```

---

## 📦 Requirements

### Server

- Python 3.9+  
- ClickHouse  
- GeoLite2 databases:  
  - `GeoLite2-City.mmdb`  
  - `GeoLite2-ASN.mmdb`  

Python dependencies:

```bash
pip install clickhouse-driver geoip2
```

---

### Client

- Python 3.9+  
- Access to `/var/log/ufw.log`  

---

## ⚙️ Configuration

### Server

Edit variables in the source:

```python
clickhouseHost = '127.0.0.1'
clickhousePort = 9000

queueMaxSize = 50000
batchSize = 500
workerCount = 1
flushInterval = 1.0
```

TLS certificates:

```
certs/server.crt
certs/server.key
```

---

### Client

```python
logFile = '/var/log/ufw.log'

serverHost = 'your.server.host'
serverPort = 9050
```

TLS:

```
server.crt
```

---

## 🧠 Data Flow

1. Agent reads new lines from UFW log  
2. Sends:
   ```
   <hostname> <raw log line>
   ```
3. Collector:
   - Parses fields via regex  
   - Extracts IPs, ports, protocol, interfaces  
   - Enriches with GeoIP + ASN  
4. Data is batched and inserted into ClickHouse  

---

## 🗄 ClickHouse Schema (example)

```sql
CREATE TABLE logs
(
    id UInt64,

    time UInt32,
    dt DateTime MATERIALIZED toDateTime(time),

    client LowCardinality(String),
    host LowCardinality(String),
    action LowCardinality(String),

    src IPv4,
    dst IPv4,
    proto LowCardinality(String),

    srcPort UInt16,
    dstPort UInt16,

    inInterface LowCardinality(String),
    outInterface LowCardinality(String),

    country FixedString(2),
    lat Float32,
    lon Float32,

    asn UInt32,
    org LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toDate(dt)
ORDER BY (dt, action, src, dstPort)
SETTINGS index_granularity = 8192;
```

---

## 🌍 GeoIP Enrichment

The system uses MaxMind GeoLite2:

- Country code  
- Latitude / Longitude  
- ASN number  
- Organization  

### Auto-update

- Runs every **12 hours**  
- Downloads fresh `.mmdb` files  
- Hot-swaps databases without downtime  
- Clears cache (`lru_cache`)  

---

## ⚡ Performance

- Batch inserts (default: 500 rows)  
- Queue buffering (50k events)  
- LRU cache for IP enrichment (100k entries)  
- Minimal allocations in hot path  

---

## 🔁 Reliability

- Retry on ClickHouse insert (3 attempts)  
- Client auto-reconnect with exponential backoff  
- Queue overflow protection (drop with metrics)  
- Graceful shutdown via signals  

---

## 📊 Metrics

Client logs example:

```
Queue size=123 dropped=0
```

Server logs example:

```
Worker 0: inserted 500
Queue full — dropping event
```

---

## 🔒 Security

- TLS encryption required  
- Certificate verification on client  
- No plaintext transport  

---

## ▶️ Running

### Start Server

```bash
python server.py
```

### Start Client

```bash
python client.py
```

---

## ⚠️ Notes

- Ensure UFW logging is enabled:
  ```bash
  sudo ufw logging on
  ```
- Log format must match standard UFW output  
- High traffic may require tuning:
  - `workerCount`
  - `batchSize`

---

## 📌 TODO / Ideas

- Prometheus metrics  
- Kafka support  
- Compression (zstd)  
- IPv6 full support  
- Web dashboard  

---

## 📄 License

MIT
