# Panduan Setup Lab Modul 8 — Analitik Aliran Data
> Apache Kafka + Spark Structured Streaming · KRaft Mode · Docker

---

## Daftar Isi

1. [Prasyarat](#1-prasyarat)
2. [Struktur Folder](#2-struktur-folder)
3. [Membuat Semua File Konfigurasi](#3-membuat-semua-file-konfigurasi)
4. [Menjalankan Lingkungan Docker](#4-menjalankan-lingkungan-docker)
5. [Verifikasi Kafka Berjalan](#5-verifikasi-kafka-berjalan)
6. [Mengakses Kafka UI](#6-mengakses-kafka-ui)
7. [Mengakses Spark UI](#7-mengakses-spark-ui)
8. [Persiapan Data & Inisialisasi untuk Latihan](#8-persiapan-data--inisialisasi-untuk-latihan)
9. [Checklist Sebelum Memulai Latihan](#9-checklist-sebelum-memulai-latihan)

---

## 1. Prasyarat

Pastikan perangkat lunak berikut sudah terpasang di laptop/PC Anda sebelum memulai.

| Perangkat Lunak | Versi Minimum | Cara Cek |
|---|---|---|
| Docker Engine | 24.0 | `docker --version` |
| Docker Compose | 2.20 | `docker compose version` |
| Python | 3.10 | `python --version` |
| Java (JDK/JRE) | 11 | `java -version` |
| Git | 2.x | `git --version` |

> **Catatan untuk Windows:** Gunakan WSL2 (Windows Subsystem for Linux) agar semua perintah bash berjalan konsisten. Pastikan Docker Desktop diintegrasikan dengan WSL2.

---

## 2. Struktur Folder

Berikut adalah struktur folder lengkap yang akan kita buat dari nol.

```
praktikum-bigdata/
└── modul8/
    ├── docker-compose.yml          ← Kafka broker (KRaft) + Kafka UI
    ├── requirements.txt            ← Dependensi Python
    ├── scripts/
    │   ├── init_topics.sh          ← Script inisialisasi topic Kafka
    │   ├── producer_transaksi.py   ← Producer: generate event transaksi
    │   └── producer_sensor_iot.py  ← Producer: generate event sensor IoT
    ├── spark/
    │   └── streaming_agregasi.py   ← Pipeline Spark Structured Streaming
    ├── data/
    │   └── sample_events.json      ← Sample data statis untuk referensi
    └── README.md                   ← Instruksi singkat untuk mahasiswa
```

Buat struktur ini dengan perintah berikut:

```bash
mkdir -p praktikum-bigdata/modul8/{scripts,spark,data}
cd praktikum-bigdata/modul8
```

---

## 3. Membuat Semua File Konfigurasi

Jalankan perintah di bawah satu per satu dari dalam direktori `modul8/`.

### 3.1 `docker-compose.yml`

File ini menjalankan dua layanan:
- **kafka-broker** — Apache Kafka 3.7 dalam mode KRaft (tanpa ZooKeeper)
- **kafka-ui** — Antarmuka web Kafka UI untuk memantau topic, partisi, dan pesan

```bash
cat > docker-compose.yml << 'EOF'
services:

  kafka-broker:
    image: apache/kafka:3.7.0
    container_name: kafka-broker
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092"]
      interval: 15s
      timeout: 10s
      retries: 5

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local-cluster
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-broker:9092
    depends_on:
      kafka-broker:
        condition: service_healthy
EOF
```

> **Mengapa dua service?**  
> `kafka-broker` adalah engine utama. `kafka-ui` adalah dashboard web sehingga mahasiswa bisa memantau topic, melihat pesan, dan mengamati offset tanpa perlu CLI.

---

### 3.2 `requirements.txt`

```bash
cat > requirements.txt << 'EOF'
kafka-python==2.0.2
pyspark==3.5.5
EOF
```

---

### 3.3 `scripts/init_topics.sh`

Script ini dijalankan **sekali** setelah Kafka broker aktif, untuk membuat semua topic yang dibutuhkan latihan.

```bash
cat > scripts/init_topics.sh << 'EOF'
#!/bin/bash
# =============================================================
# init_topics.sh — Inisialisasi topic Kafka untuk Modul 8
# Jalankan: bash scripts/init_topics.sh
# =============================================================

BROKER="localhost:9092"
CONTAINER="kafka-broker"

echo "======================================"
echo " Inisialisasi Topic Kafka — Modul 8"
echo "======================================"

echo "[1/3] Membuat topic: transaksi-stream (3 partisi)..."
docker exec $CONTAINER kafka-topics.sh --create \
  --bootstrap-server $BROKER \
  --topic transaksi-stream \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "[2/3] Membuat topic: sensor-iot (2 partisi)..."
docker exec $CONTAINER kafka-topics.sh --create \
  --bootstrap-server $BROKER \
  --topic sensor-iot \
  --partitions 2 \
  --replication-factor 1 \
  --if-not-exists

echo "[3/3] Membuat topic: penjualan-agregat (1 partisi)..."
docker exec $CONTAINER kafka-topics.sh --create \
  --bootstrap-server $BROKER \
  --topic penjualan-agregat \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo ""
echo "--- Verifikasi semua topic ---"
docker exec $CONTAINER kafka-topics.sh --list \
  --bootstrap-server $BROKER

echo ""
echo "--- Detail topic transaksi-stream ---"
docker exec $CONTAINER kafka-topics.sh --describe \
  --bootstrap-server $BROKER \
  --topic transaksi-stream

echo ""
echo "[OK] Semua topic berhasil dibuat."
EOF

chmod +x scripts/init_topics.sh
```

---

### 3.4 `scripts/producer_transaksi.py`

Producer ini menghasilkan event transaksi e-commerce secara kontinu. Digunakan di **Tahap 2 dan 3** latihan.

```bash
cat > scripts/producer_transaksi.py << 'EOF'
"""
producer_transaksi.py
Menghasilkan event transaksi e-commerce acak ke topic 'transaksi-stream'.
Jalankan: python scripts/producer_transaksi.py
Hentikan: Ctrl+C
"""

import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME   = "transaksi-stream"

CHANNELS  = ["mobile", "web", "atm", "teller"]
PRODUCTS  = ["elektronik", "fashion", "makanan", "kesehatan", "otomotif"]
USER_IDS  = [f"usr-{i:04d}" for i in range(1, 51)]

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
    enable_idempotence=True,
    retries=3,
)

def buat_event():
    user_id    = random.choice(USER_IDS)
    event_time = datetime.now(timezone.utc)
    return {
        "event_id":   str(uuid.uuid4())[:8],
        "user_id":    user_id,
        "product":    random.choice(PRODUCTS),
        "channel":    random.choice(CHANNELS),
        "amount":     round(random.uniform(10_000, 5_000_000), 2),
        "event_time": event_time.isoformat(),
    }, user_id

def main():
    print(f"[Producer] Mengirim event ke '{TOPIC_NAME}'... (Ctrl+C untuk berhenti)")
    sent = 0
    try:
        while True:
            event, key = buat_event()
            producer.send(TOPIC_NAME, key=key, value=event)
            sent += 1
            if sent % 10 == 0:
                print(f"  [{sent:>4} event terkirim] "
                      f"id={event['event_id']} | "
                      f"user={key} | "
                      f"channel={event['channel']} | "
                      f"amount={event['amount']:>12,.0f}")
            time.sleep(random.uniform(0.1, 0.5))
    except KeyboardInterrupt:
        print(f"\n[Producer] Dihentikan. Total terkirim: {sent}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
EOF
```

---

### 3.5 `scripts/producer_sensor_iot.py`

Producer kedua untuk topic `sensor-iot`. Digunakan di eksplorasi mandiri atau latihan lanjutan.

```bash
cat > scripts/producer_sensor_iot.py << 'EOF'
"""
producer_sensor_iot.py
Menghasilkan event pembacaan sensor IoT ke topic 'sensor-iot'.
Jalankan: python scripts/producer_sensor_iot.py
Hentikan: Ctrl+C
"""

import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME   = "sensor-iot"

SENSOR_IDS = [f"sensor-{i:03d}" for i in range(1, 21)]
LOCATIONS  = ["gudang-A", "gudang-B", "lantai-1", "lantai-2", "parkir"]

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",
)

def buat_event():
    sensor_id  = random.choice(SENSOR_IDS)
    event_time = datetime.now(timezone.utc)
    return {
        "event_id":    str(uuid.uuid4())[:8],
        "sensor_id":   sensor_id,
        "location":    random.choice(LOCATIONS),
        "temperature": round(random.uniform(20.0, 45.0), 2),
        "humidity":    round(random.uniform(30.0, 90.0), 2),
        "status":      random.choice(["normal", "normal", "normal", "warning", "critical"]),
        "event_time":  event_time.isoformat(),
    }, sensor_id

def main():
    print(f"[Producer IoT] Mengirim ke '{TOPIC_NAME}'... (Ctrl+C untuk berhenti)")
    sent = 0
    try:
        while True:
            event, key = buat_event()
            producer.send(TOPIC_NAME, key=key, value=event)
            sent += 1
            if sent % 5 == 0:
                print(f"  [{sent:>4}] {key} | "
                      f"temp={event['temperature']}°C | "
                      f"status={event['status']}")
            time.sleep(random.uniform(0.2, 1.0))
    except KeyboardInterrupt:
        print(f"\n[Producer IoT] Dihentikan. Total: {sent}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
EOF
```

---

### 3.6 `spark/streaming_agregasi.py`

Pipeline utama Spark Structured Streaming. Digunakan di **Tahap 3 dan 4** latihan.

```bash
cat > spark/streaming_agregasi.py << 'EOF'
"""
streaming_agregasi.py
Pipeline Spark Structured Streaming: baca dari Kafka,
agregasi per window, tulis ke console.

Jalankan:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
    --master local[2] \
    --conf spark.sql.shuffle.partitions=4 \
    spark/streaming_agregasi.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType,
)

KAFKA_SERVERS   = "localhost:9092"
TOPIC_IN        = "transaksi-stream"
CHECKPOINT_DIR  = "/tmp/checkpoints/streaming-agregasi"

schema_event = StructType([
    StructField("event_id",   StringType(),   True),
    StructField("user_id",    StringType(),   True),
    StructField("product",    StringType(),   True),
    StructField("channel",    StringType(),   True),
    StructField("amount",     DoubleType(),   True),
    StructField("event_time", TimestampType(), True),
])

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("StreamingAgregatWindow")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Baca raw bytes dari Kafka ---
    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC_IN)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 100)
        .option("failOnDataLoss", "false")
        .load()
    )

    # --- Parse JSON payload ---
    df = (
        df_raw
        .select(
            F.from_json(F.col("value").cast("string"), schema_event).alias("d"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
        )
        .select("d.*", "kafka_partition", "kafka_offset")
    )

    # --- Query 1: Total penjualan per channel per tumbling window 1 menit ---
    df_q1 = (
        df
        .withWatermark("event_time", "2 minutes")
        .groupBy(F.window("event_time", "1 minute"), "channel")
        .agg(
            F.count("*").alias("jumlah"),
            F.sum("amount").alias("total"),
            F.avg("amount").alias("rata_rata"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "channel", "jumlah",
            F.round("total", 0).alias("total"),
            F.round("rata_rata", 0).alias("rata_rata"),
        )
    )

    # --- Query 2: Top user berdasarkan total volume (tanpa window) ---
    df_q2 = (
        df
        .groupBy("user_id")
        .agg(
            F.count("*").alias("total_transaksi"),
            F.sum("amount").alias("total_amount"),
        )
        .orderBy(F.col("total_amount").desc())
    )

    # --- Tulis Query 1 ke console (update mode, setiap 15 detik) ---
    q1 = (
        df_q1.writeStream
        .queryName("penjualan_per_channel")
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .option("checkpointLocation", CHECKPOINT_DIR + "/q1")
        .trigger(processingTime="15 seconds")
        .start()
    )

    # --- Tulis Query 2 ke console (complete mode, setiap 30 detik) ---
    q2 = (
        df_q2.writeStream
        .queryName("top_user")
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 5)
        .option("checkpointLocation", CHECKPOINT_DIR + "/q2")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print("\n[Streaming] Pipeline aktif.")
    print("[Streaming] Spark UI → http://localhost:4040")
    print("[Streaming] Kafka UI → http://localhost:8080")
    print("[Streaming] Tekan Ctrl+C untuk berhenti.\n")

    spark.streams.awaitAnyTermination()
EOF
```

---

### 3.7 `data/sample_events.json`

Data statis berisi 10 contoh event. Berguna sebagai referensi skema dan untuk pengujian manual.

```bash
cat > data/sample_events.json << 'EOF'
[
  {"event_id":"a1b2c3d4","user_id":"usr-0001","product":"elektronik","channel":"mobile","amount":1250000.00,"event_time":"2024-04-15T08:00:00Z"},
  {"event_id":"b2c3d4e5","user_id":"usr-0002","product":"fashion","channel":"web","amount":350000.00,"event_time":"2024-04-15T08:00:05Z"},
  {"event_id":"c3d4e5f6","user_id":"usr-0003","product":"makanan","channel":"atm","amount":75000.00,"event_time":"2024-04-15T08:00:10Z"},
  {"event_id":"d4e5f6g7","user_id":"usr-0001","product":"kesehatan","channel":"mobile","amount":220000.00,"event_time":"2024-04-15T08:00:20Z"},
  {"event_id":"e5f6g7h8","user_id":"usr-0004","product":"otomotif","channel":"teller","amount":4500000.00,"event_time":"2024-04-15T08:00:30Z"},
  {"event_id":"f6g7h8i9","user_id":"usr-0005","product":"elektronik","channel":"web","amount":899000.00,"event_time":"2024-04-15T08:00:40Z"},
  {"event_id":"g7h8i9j0","user_id":"usr-0002","product":"fashion","channel":"mobile","amount":175000.00,"event_time":"2024-04-15T08:00:50Z"},
  {"event_id":"h8i9j0k1","user_id":"usr-0006","product":"makanan","channel":"web","amount":45000.00,"event_time":"2024-04-15T08:01:00Z"},
  {"event_id":"i9j0k1l2","user_id":"usr-0007","product":"kesehatan","channel":"atm","amount":320000.00,"event_time":"2024-04-15T08:01:15Z"},
  {"event_id":"j0k1l2m3","user_id":"usr-0003","product":"otomotif","channel":"teller","amount":2750000.00,"event_time":"2024-04-15T08:01:30Z"}
]
EOF
```

---

### 3.8 `README.md`

```bash
cat > README.md << 'EOF'
# Modul 8 — Analitik Aliran Data

## Langkah Cepat

```bash
# 1. Jalankan Kafka
docker compose up -d

# 2. Buat topic
bash scripts/init_topics.sh

# 3. Install dependensi Python
pip install -r requirements.txt

# 4. Jalankan producer (Terminal 1)
python scripts/producer_transaksi.py

# 5. Jalankan Spark Streaming (Terminal 2)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --master local[2] \
  spark/streaming_agregasi.py
```

## UI

| Dashboard | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Spark UI | http://localhost:4040 |

## Menghentikan Lingkungan

```bash
docker compose down        # hentikan, simpan data
docker compose down -v     # hentikan + hapus volume
```
EOF
```

---

## 4. Menjalankan Lingkungan Docker

### Langkah 4.1 — Jalankan Kafka broker dan Kafka UI

```bash
# Dari dalam direktori modul8/
docker compose up -d
```

Tunggu sekitar **30–60 detik** hingga semua container sehat. Cek status:

```bash
docker compose ps
```

Output yang diharapkan:

```
NAME           IMAGE                          STATUS
kafka-broker   apache/kafka:3.7.0             Up (healthy)
kafka-ui       provectuslabs/kafka-ui:latest  Up
```

> Jika `kafka-broker` masih dalam status `starting`, tunggu beberapa detik lagi lalu cek ulang. Kafka butuh waktu inisialisasi KRaft pertama kali.

### Langkah 4.2 — Install dependensi Python

```bash
pip install -r requirements.txt
```

---

## 5. Verifikasi Kafka Berjalan

### Langkah 5.1 — Buat semua topic sekaligus

```bash
bash scripts/init_topics.sh
```

Output yang diharapkan di akhir script:

```
penjualan-agregat
sensor-iot
transaksi-stream

[OK] Semua topic berhasil dibuat.
```

### Langkah 5.2 — Cek detail partisi dan replikasi

```bash
docker exec kafka-broker kafka-topics.sh \
  --describe \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream
```

Output contoh:

```
Topic: transaksi-stream  Partitions: 3  ReplicationFactor: 1
  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

### Langkah 5.3 — Uji kirim dan terima pesan manual

Buka **dua terminal terpisah**.

**Terminal A — Consumer (mendengarkan):**
```bash
docker exec -it kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream \
  --from-beginning
```

**Terminal B — Producer (mengirim):**
```bash
docker exec -it kafka-broker kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream
```

Ketik sembarang teks di Terminal B, tekan Enter. Teks harus langsung muncul di Terminal A. Ini membuktikan Kafka berjalan dengan benar.

Hentikan kedua terminal dengan `Ctrl+C`.

---

## 6. Mengakses Kafka UI

Buka browser dan arahkan ke:

```
http://localhost:8080
```

Yang bisa dilakukan dari Kafka UI:

| Menu | Fungsi |
|---|---|
| **Dashboard** | Overview cluster, jumlah broker aktif |
| **Topics** | Daftar topic, jumlah partisi, ukuran log |
| **Topics → transaksi-stream → Messages** | Lihat isi pesan secara real-time |
| **Topics → transaksi-stream → Statistics** | Grafik throughput masuk/keluar |
| **Consumer Groups** | Pantau offset lag setiap consumer group |

> Kafka UI akan menampilkan pesan masuk secara live saat producer Python berjalan nanti.

---

## 7. Mengakses Spark UI

Spark UI **hanya aktif saat ada job Spark yang berjalan**. UI akan tersedia di:

```
http://localhost:4040
```

### Langkah 7.1 — Jalankan producer Python terlebih dahulu

Buka Terminal baru (Terminal 1):

```bash
python scripts/producer_transaksi.py
```

Biarkan producer berjalan. Lanjut ke langkah berikut.

### Langkah 7.2 — Jalankan Spark Streaming

Buka Terminal baru lagi (Terminal 2):

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --master local[2] \
  --conf spark.sql.shuffle.partitions=4 \
  spark/streaming_agregasi.py
```

Spark akan mengunduh paket Kafka (`spark-sql-kafka`) otomatis pada **pertama kali dijalankan** — butuh koneksi internet dan sekitar 1–3 menit. Unduhan berikutnya menggunakan cache lokal.

Setelah muncul pesan `[Streaming] Pipeline aktif.`, buka browser ke:

```
http://localhost:4040
```

### Langkah 7.3 — Navigasi Spark UI

| Tab | Yang Diamati |
|---|---|
| **Structured Streaming** | Input rate, Processing rate, status query aktif |
| **Jobs** | Daftar job per micro-batch yang sudah selesai |
| **Stages** | Breakdown tahap eksekusi per micro-batch |
| **Storage** | RDD / DataFrame yang di-cache |
| **Environment** | Konfigurasi Spark yang aktif |

Klik nama query (`penjualan_per_channel` atau `top_user`) di tab **Structured Streaming** untuk melihat grafik event rate secara real-time.

---

## 8. Persiapan Data & Inisialisasi untuk Latihan

### Data yang Digunakan

Modul 8 menggunakan **data sintetis yang di-generate secara real-time** oleh producer Python, bukan file statis. Berikut ringkasannya:

| Topic Kafka | Producer | Schema |
|---|---|---|
| `transaksi-stream` | `producer_transaksi.py` | event_id, user_id, product, channel, amount, event_time |
| `sensor-iot` | `producer_sensor_iot.py` | event_id, sensor_id, location, temperature, humidity, status, event_time |
| `penjualan-agregat` | *(ditulis oleh Spark)* | Hasil agregasi dari transaksi-stream |

File `data/sample_events.json` berisi **10 event contoh statis** sebagai referensi skema — tidak perlu di-load ke Kafka, cukup dibaca mahasiswa untuk memahami struktur data.

### Inisialisasi Data Latihan — Step by Step

Lakukan langkah ini **setiap kali memulai sesi baru** atau setelah `docker compose down -v`.

**Step 1 — Pastikan Kafka berjalan:**
```bash
docker compose ps
# Pastikan kafka-broker STATUS = Up (healthy)
```

**Step 2 — Buat ulang topic (jika perlu):**
```bash
bash scripts/init_topics.sh
```

**Step 3 — Kirim data awal ke Kafka (pre-seed)**

Jalankan producer selama minimal **60 detik** sebelum mahasiswa mulai agar ada data historis di topic:

```bash
# Kirim ~200 event awal, lalu hentikan dengan Ctrl+C
python scripts/producer_transaksi.py
```

> Dengan kecepatan 0.1–0.5 detik per event, 60 detik menghasilkan sekitar 120–600 event. Ini cukup untuk mengisi beberapa window dalam latihan Tahap 3.

**Step 4 — Verifikasi data sudah masuk ke Kafka:**
```bash
docker exec kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream \
  --from-beginning \
  --max-messages 5
```

Output yang diharapkan (contoh):
```json
{"event_id":"3a9f21b0","user_id":"usr-0023","product":"elektronik","channel":"web","amount":872345.00,"event_time":"2024-04-15T10:01:05.123456+00:00"}
```

**Step 5 — Cek jumlah offset di setiap partisi:**
```bash
docker exec kafka-broker kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream
```

Output contoh:
```
transaksi-stream:0:87
transaksi-stream:1:84
transaksi-stream:2:89
```
Angka di belakang `:` adalah jumlah event yang tersimpan di partisi tersebut.

**Step 6 — Data siap.** Mahasiswa bisa memulai Tahap 2 latihan dengan menjalankan producer secara live bersamaan dengan Spark Streaming.

### Reset Data (untuk percobaan ulang)

Jika ingin mengosongkan semua data dan mulai dari nol:

```bash
# Hapus semua container + volume data Kafka
docker compose down -v

# Jalankan ulang
docker compose up -d

# Buat ulang topic
bash scripts/init_topics.sh
```

---

## 9. Checklist Sebelum Memulai Latihan

Gunakan checklist ini untuk memastikan lingkungan siap sebelum membagikan ke mahasiswa.

- [ ] `docker compose ps` menampilkan `kafka-broker` status **Up (healthy)**
- [ ] `docker compose ps` menampilkan `kafka-ui` status **Up**
- [ ] `bash scripts/init_topics.sh` selesai tanpa error
- [ ] Kafka UI terbuka di `http://localhost:8080` dan menampilkan 3 topic
- [ ] `python scripts/producer_transaksi.py` berjalan dan mencetak event ke terminal
- [ ] Event muncul di Kafka UI → Topics → `transaksi-stream` → Messages
- [ ] `spark-submit` berhasil menjalankan `streaming_agregasi.py`
- [ ] Spark UI terbuka di `http://localhost:4040` dan menampilkan query aktif
- [ ] Output tabel muncul di terminal Spark setiap 15 detik

Jika semua centang terpenuhi, lingkungan lab **siap digunakan** untuk seluruh tahap latihan di Modul 8.