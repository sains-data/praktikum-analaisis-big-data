# Panduan Setup Lab Modul 7 — Orkestrasi Alur Kerja dan Tata Kelola Big Data
> Apache Airflow 2.7.3 · Apache Atlas 2.3.0 · Spark + Hive + HDFS · Virtual Environment Python

> **Catatan Arsitektur:** Modul 7 **tidak menggunakan Docker Compose baru**.
> Airflow dan Atlas diinstal **di dalam** kontainer `bigdata-spark` yang sudah
> ada dari Modul 9, karena keduanya membutuhkan akses langsung ke Spark, Hive,
> dan HDFS. Tidak ada container tambahan — semua komponen berjalan di satu host.

---

## Daftar Isi

1. [Prasyarat](#1-prasyarat)
2. [Struktur Folder](#2-struktur-folder)
3. [Instalasi Apache Airflow](#3-instalasi-apache-airflow)
4. [Instalasi Apache Atlas](#4-instalasi-apache-atlas)
5. [Konfigurasi Koneksi Airflow ke Spark dan Hive](#5-konfigurasi-koneksi-airflow-ke-spark-dan-hive)
6. [Menjalankan Semua Layanan](#6-menjalankan-semua-layanan)
7. [Mengakses UI Airflow dan Atlas](#7-mengakses-ui-airflow-dan-atlas)
8. [Verifikasi Keseluruhan Lingkungan](#8-verifikasi-keseluruhan-lingkungan)
9. [Persiapan Data dan Inisialisasi untuk Latihan](#9-persiapan-data-dan-inisialisasi-untuk-latihan)
10. [Checklist Sebelum Memulai Latihan](#10-checklist-sebelum-memulai-latihan)

---

## 1. Prasyarat

| Komponen | Versi | Status yang Dibutuhkan |
|---|---|---|
| Kontainer `bigdata-spark` | Dari Modul 9 | Berjalan (`docker ps`) |
| Hadoop HDFS + YARN | 3.4.1 | `jps` menampilkan 4 proses |
| Apache Spark | 3.5.5 | `spark-submit --version` |
| Apache Hive | 3.x | `hive --version` |
| Python | 3.10+ | `python3 --version` |
| RAM tersedia di kontainer | Min 6 GB | Airflow + Atlas berjalan bersamaan |
| Koneksi internet | Diperlukan | Untuk unduh Airflow dan Atlas |

> **Urutan modul:** Modul 7 menggunakan infrastruktur yang sudah ada dari Modul 9
> (`bigdata-spark`). Jika Modul 9 belum pernah dijalankan, mulai dari setup
> `bigdata-spark` di panduan Modul 9 terlebih dahulu, lalu kembali ke sini.

---

## 2. Struktur Folder

Semua direktori berikut dibuat **di dalam** kontainer `bigdata-spark`:

```
(di dalam kontainer bigdata-spark)
├── ~/airflow/                        ← AIRFLOW_HOME
│   ├── airflow.cfg                   ← konfigurasi Airflow (auto-generated)
│   ├── airflow.db                    ← SQLite metadata DB (untuk lab)
│   ├── dags/                         ← direktori DAG Python
│   │   ├── latihan_pipeline.py       ← DAG Tahap 1
│   │   ├── latihan_pipeline_spark.py ← DAG Tahap 2
│   │   └── pipeline_atlas.py         ← DAG Tahap 4
│   └── logs/                         ← log scheduler & webserver
│
├── ~/airflow-env/                    ← Python virtual environment Airflow
│
├── /opt/atlas/                       ← ATLAS_HOME (symlink ke atlas-2.3.0)
│   ├── bin/
│   ├── conf/
│   │   ├── atlas-application.properties
│   │   └── users-credentials.properties
│   ├── data/
│   └── logs/
│
├── /opt/spark-jobs/                  ← Spark job scripts untuk Airflow
│   ├── latihan_etl.py                ← ETL Bronze → Silver (Tahap 2)
│   └── pipeline_gold.py              ← Agregasi Silver → Gold (Tahap 4)
│
└── /opt/scripts/                     ← Helper scripts
    └── generate_data.py              ← Generator data transaksi CSV
```

Di HDFS (struktur yang dibutuhkan latihan):

```
hdfs:///
├── datalake/
│   ├── bronze/
│   │   └── latihan/                  ← file CSV hasil ingest DAG
│   ├── silver/
│   │   └── latihan/                  ← file Parquet hasil Spark ETL
│   └── gold/
│       └── latihan/                  ← tabel agregat akhir
└── spark-logs/                       ← event log Spark
```

---

## 3. Instalasi Apache Airflow

Semua langkah dijalankan **di dalam** kontainer `bigdata-spark`.

### Langkah 3.1 — Masuk ke kontainer

```bash
# Dari WSL Ubuntu / terminal host
bash start.sh    # dari direktori bigdata-spark/
bash login.sh
```

Verifikasi Hadoop berjalan:

```bash
jps
# Harus ada: NameNode, DataNode, ResourceManager, NodeManager
```

Jika belum aktif:

```bash
start-dfs.sh && start-yarn.sh && sleep 10 && jps
```

---

### Langkah 3.2 — Buat virtual environment Python untuk Airflow

```bash
# Buat virtual environment
python3 -m venv ~/airflow-env

# Aktifkan
source ~/airflow-env/bin/activate

# Verifikasi Python di venv
which python && python --version
```

---

### Langkah 3.3 — Tentukan URL constraint dan instal Airflow

```bash
# Deteksi versi Python yang sedang digunakan
PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "Python version: $PYTHON_VERSION"

# Constraint URL (menjamin kompatibilitas dependensi)
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-${PYTHON_VERSION}.txt"
echo "Constraint URL: $CONSTRAINT_URL"

# Instal Airflow + provider Spark + provider Hive + requests
pip install "apache-airflow==2.7.3" \
  apache-airflow-providers-apache-spark \
  apache-airflow-providers-apache-hive \
  requests \
  --constraint "$CONSTRAINT_URL"
```

Proses instalasi membutuhkan **5–10 menit** tergantung kecepatan internet.

Verifikasi instalasi:

```bash
airflow version
# Output yang diharapkan: 2.7.3
```

---

### Langkah 3.4 — Set AIRFLOW_HOME dan tambahkan ke .bashrc

```bash
export AIRFLOW_HOME=~/airflow
echo "export AIRFLOW_HOME=~/airflow" >> ~/.bashrc
echo "source ~/airflow-env/bin/activate" >> ~/.bashrc

# Verifikasi
echo $AIRFLOW_HOME
```

---

### Langkah 3.5 — Inisialisasi database metadata Airflow

```bash
# Inisialisasi database (SQLite — cukup untuk lingkungan lab)
airflow db init
```

Output yang diharapkan di akhir proses:

```
DB: sqlite:////root/airflow/airflow.db
[2024-xx-xx] {migration.py:xxx} INFO - Running upgrade ...
Initialization done
```

---

### Langkah 3.6 — Buat akun admin Airflow

```bash
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname Lab \
  --role Admin \
  --email admin@lab.local \
  --password admin123
```

Output yang diharapkan:

```
[2024-xx-xx] {manager.py:xxx} INFO - Added user admin
User "admin" created with role "Admin"
```

---

### Langkah 3.7 — Buat direktori kerja

```bash
# Direktori DAG, log, spark jobs, dan scripts
mkdir -p ~/airflow/dags
mkdir -p ~/airflow/logs
mkdir -p /opt/spark-jobs
mkdir -p /opt/scripts

# Verifikasi
ls -la ~/airflow/
ls -la /opt/spark-jobs /opt/scripts
```

---

## 4. Instalasi Apache Atlas

### Langkah 4.1 — Unduh paket Atlas biner

```bash
# Unduh Atlas 2.3.0 (paket sudah termasuk Solr dan BerkeleyDB embedded)
wget https://downloads.apache.org/atlas/2.3.0/apache-atlas-2.3.0-bin.tar.gz \
  -O /opt/apache-atlas-2.3.0-bin.tar.gz

# Verifikasi ukuran file (~800 MB)
ls -lh /opt/apache-atlas-2.3.0-bin.tar.gz
```

> Jika unduhan lambat, gunakan mirror Apache:
> ```bash
> wget https://archive.apache.org/dist/atlas/2.3.0/apache-atlas-2.3.0-bin.tar.gz \
>   -O /opt/apache-atlas-2.3.0-bin.tar.gz
> ```

---

### Langkah 4.2 — Ekstrak dan buat symlink

```bash
# Ekstrak ke /opt/
tar -xzf /opt/apache-atlas-2.3.0-bin.tar.gz -C /opt/

# Buat symlink agar path konsisten
ln -sf /opt/apache-atlas-2.3.0 /opt/atlas

# Verifikasi
ls -la /opt/atlas/
```

---

### Langkah 4.3 — Set variabel lingkungan Atlas

```bash
export ATLAS_HOME=/opt/atlas
echo "export ATLAS_HOME=/opt/atlas" >> ~/.bashrc
echo 'export PATH=$PATH:$ATLAS_HOME/bin' >> ~/.bashrc

# Reload .bashrc
source ~/.bashrc

# Verifikasi
echo $ATLAS_HOME
which atlas_start.py
```

---

### Langkah 4.4 — Konfigurasi `atlas-application.properties`

Buka file konfigurasi:

```bash
nano /opt/atlas/conf/atlas-application.properties
```

Cari dan pastikan properti-properti berikut bernilai seperti di bawah ini
(edit jika berbeda, tambahkan jika belum ada):

```properties
# ── Backend penyimpanan graf (BerkeleyDB — embedded, tidak perlu install) ──
atlas.graph.storage.backend=berkeleyje
atlas.graph.storage.directory=/opt/atlas/data/berkley

# ── Mesin pencari (Solr embedded — tidak perlu install Solr terpisah) ──
atlas.graph.index.search.backend=solr
atlas.graph.index.search.solr.mode=embedded
atlas.graph.index.search.solr.zookeeper-url=

# ── Alamat REST API Atlas ──
atlas.rest.address=http://localhost:21000

# ── Autentikasi berbasis file (cukup untuk lab) ──
atlas.authentication.method.file=true
atlas.authentication.method.file.filename=${sys:atlas.home}/conf/users-credentials.properties
atlas.authentication.method.ldap.type=none

# ── Server settings ──
atlas.server.http.port=21000
atlas.enableTLS=false
```

Simpan file: `Ctrl+O` → `Enter` → `Ctrl+X`

---

### Langkah 4.5 — Buat direktori data Atlas

```bash
mkdir -p /opt/atlas/data/berkley
mkdir -p /opt/atlas/data/solr
mkdir -p /opt/atlas/logs

# Verifikasi
ls -la /opt/atlas/data/
```

---

### Langkah 4.6 — Aktifkan Hive Hook untuk lineage otomatis

```bash
# Salin jar Atlas Hive Hook ke direktori lib Hive
cp /opt/atlas/hook/hive/*.jar $HIVE_HOME/lib/

# Verifikasi jar tersalin
ls /opt/atlas/hook/hive/*.jar | wc -l
```

Tambahkan konfigurasi hook ke `hive-site.xml`:

```bash
cat >> $HIVE_HOME/conf/hive-site.xml << 'HIVEEOF'
<property>
  <name>hive.exec.post.hooks</name>
  <value>org.apache.atlas.hive.hook.HiveHook</value>
</property>
<property>
  <name>hive.reloadable.aux.jars.path</name>
  <value>/opt/atlas/hook/hive</value>
</property>
HIVEEOF
```

Tambahkan path jar ke `hive-env.sh`:

```bash
echo "export HIVE_AUX_JARS_PATH=/opt/atlas/hook/hive" \
  >> $HIVE_HOME/conf/hive-env.sh
```

Salin konfigurasi Atlas ke direktori conf Hive agar Hook bisa terhubung ke server:

```bash
cp /opt/atlas/conf/atlas-application.properties \
   $HIVE_HOME/conf/
```

---

## 5. Konfigurasi Koneksi Airflow ke Spark dan Hive

Pastikan virtual environment Airflow aktif:

```bash
source ~/airflow-env/bin/activate
```

### Langkah 5.1 — Daftarkan koneksi ke Spark cluster (YARN)

```bash
airflow connections add spark_default \
  --conn-type spark \
  --conn-host yarn \
  --conn-extra '{
    "deploy-mode": "client",
    "spark-home": "/opt/spark",
    "spark-binary": "spark-submit",
    "queue": "default"
  }'

# Verifikasi
airflow connections get spark_default
```

### Langkah 5.2 — Daftarkan koneksi ke HiveServer2

```bash
# Koneksi untuk HiveOperator (eksekusi HiveQL)
airflow connections add hive_default \
  --conn-type hive_metastore \
  --conn-host localhost \
  --conn-port 10000 \
  --conn-login hive \
  --conn-password hive

# Koneksi untuk HiveServer2 via JDBC
airflow connections add hiveserver2_default \
  --conn-type hiveserver2 \
  --conn-host localhost \
  --conn-port 10000 \
  --conn-login hive \
  --conn-password hive

# Verifikasi semua koneksi terdaftar
airflow connections list | grep -E "spark|hive"
```

Output yang diharapkan:

```
spark_default       spark          yarn
hive_default        hive_metastore localhost
hiveserver2_default hiveserver2    localhost
```

---

## 6. Menjalankan Semua Layanan

### Langkah 6.1 — Jalankan Airflow Scheduler (background)

```bash
source ~/airflow-env/bin/activate

airflow scheduler \
  > ~/airflow/logs/scheduler.log 2>&1 &

echo "Scheduler PID: $!"
```

### Langkah 6.2 — Jalankan Airflow Web Server (background)

```bash
airflow webserver --port 8080 \
  > ~/airflow/logs/webserver.log 2>&1 &

echo "Web Server PID: $!"
```

Tunggu sekitar **30 detik** hingga web server siap, lalu verifikasi:

```bash
sleep 30
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/health
# Output yang diharapkan: 200
```

### Langkah 6.3 — Jalankan Apache Atlas

```bash
/opt/atlas/bin/atlas_start.py
```

Atlas membutuhkan **3–5 menit** untuk startup pertama (inisialisasi BerkeleyDB dan Solr embedded). Pantau progress:

```bash
tail -f /opt/atlas/logs/application.log | grep -E "started|ERROR|WARN" --line-buffered
```

Tunggu hingga muncul baris:

```
Atlas Server started!!!
```

Tekan `Ctrl+C` untuk berhenti memantau log.

### Langkah 6.4 — Restart HiveServer2 agar Hook aktif

```bash
# Stop HiveServer2 yang sedang berjalan
$HIVE_HOME/bin/hiveserver2 stop 2>/dev/null || true
sleep 5

# Start ulang dengan hook aktif
$HIVE_HOME/bin/hiveserver2 start &
sleep 10

# Verifikasi HiveServer2 berjalan
nc -z localhost 10000 2>/dev/null && echo "HiveServer2: OK (port 10000)" \
  || echo "HiveServer2: BELUM SIAP"
```

---

## 7. Mengakses UI Airflow dan Atlas

Buka browser di komputer **host** (Windows/Mac). Karena Airflow dan Atlas berjalan **di dalam** kontainer Docker, port-nya perlu di-expose. Pastikan kontainer sudah dikonfigurasi dengan port forwarding berikut di `docker-compose.yml` milik `bigdata-spark`:

```yaml
ports:
  - "8080:8080"    # Airflow Web UI
  - "21000:21000"  # Apache Atlas UI
  - "9870:9870"    # HDFS Web UI (sudah ada)
  - "8088:8088"    # YARN UI (sudah ada)
```

Jika port `8080` dan `21000` belum ada, **hentikan kontainer, edit `docker-compose.yml`, lalu restart**:

```bash
# Dari direktori bigdata-spark/ (di luar kontainer)
# Edit docker-compose.yml tambahkan port 8080 dan 21000
nano docker-compose.yml

# Restart kontainer
docker compose down
docker compose up -d
bash login.sh
# Jalankan ulang Airflow dan Atlas dari Langkah 6
```

### Apache Airflow Web UI

```
http://localhost:8080
```

Login: **admin / admin123**

| Menu | Fungsi |
|---|---|
| **DAGs** | Daftar semua DAG yang terdeteksi di direktori `~/airflow/dags/` |
| **DAG → Graph** | Visualisasi DAG sebagai graf dengan dependensi task |
| **DAG → Grid** | Riwayat eksekusi per DAG run (hijau = sukses, merah = gagal) |
| **DAG → Task Instance → Log** | Log detail eksekusi setiap task |
| **Admin → Connections** | Kelola koneksi ke Spark, Hive, dan sistem eksternal |
| **Admin → Variables** | Kelola variabel global yang bisa digunakan di semua DAG |
| **Browse → XComs** | Lihat nilai yang dikirim antar task via XCom |

**Test akses pertama:**
1. Login dengan `admin / admin123`
2. Di halaman DAGs, klik tombol toggle di sebelah kiri nama DAG untuk mengaktifkan
3. Klik nama DAG → tab **Graph** untuk melihat visualisasi dependensi

### Apache Atlas Web UI

```
http://localhost:21000
```

Login: **admin / admin**

| Menu | Fungsi |
|---|---|
| **Search** | Cari entitas (tabel, kolom, proses) berdasarkan nama atau tipe |
| **Entities → hive_table** | Lihat semua tabel Hive yang terdaftar |
| **Classifications** | Kelola tag/label seperti PII, FINANSIAL, SENSITIF |
| **Lineage tab** (di halaman entitas) | Visualisasi graf lineage — upstream/downstream |
| **Glossary** | Kamus istilah bisnis yang terhubung ke entitas teknis |
| **Admin** | Kelola tipe entitas dan konfigurasi server |

**Test akses pertama:**
1. Login dengan `admin / admin`
2. Klik **Search** → pilih **Entity Type: hive_table** → klik Search
3. Jika belum ada entitas, itu normal — entitas akan ditambahkan saat Latihan

---

## 8. Verifikasi Keseluruhan Lingkungan

Jalankan skrip verifikasi berikut di dalam kontainer:

```bash
source ~/airflow-env/bin/activate

echo "=== Verifikasi Lingkungan Lab Modul 7 ==="

# Airflow Scheduler
printf "%-35s" "Airflow scheduler  :"
pgrep -f "airflow scheduler" > /dev/null \
  && echo "BERJALAN" || echo "TIDAK BERJALAN"

# Airflow Web Server
printf "%-35s" "Airflow webserver  :"
pgrep -f "airflow webserver" > /dev/null \
  && echo "BERJALAN" || echo "TIDAK BERJALAN"

# Airflow UI HTTP
printf "%-35s" "Airflow UI (HTTP)  :"
CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health)
[ "$CODE" = "200" ] && echo "OK (HTTP $CODE)" || echo "GAGAL (HTTP $CODE)"

# Apache Atlas REST API
printf "%-35s" "Atlas REST API     :"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
  -u admin:admin \
  http://localhost:21000/api/atlas/admin/status)
[ "$STATUS" = "200" ] && echo "OK (HTTP $STATUS)" || echo "GAGAL (HTTP $STATUS)"

# Hadoop HDFS
printf "%-35s" "HDFS NameNode      :"
hdfs dfsadmin -report 2>/dev/null | grep "Live datanodes" \
  || echo "TIDAK BERJALAN"

# Spark
printf "%-35s" "spark-submit       :"
which spark-submit > /dev/null 2>&1 \
  && echo "DITEMUKAN ($(spark-submit --version 2>&1 | head -1))" \
  || echo "TIDAK DITEMUKAN"

# HiveServer2
printf "%-35s" "HiveServer2 (10000):"
nc -z localhost 10000 2>/dev/null \
  && echo "BERJALAN" || echo "TIDAK BERJALAN"

# Koneksi Airflow
printf "%-35s" "Koneksi Airflow    :"
COUNT=$(airflow connections list 2>/dev/null | grep -c "spark\|hive" || echo 0)
echo "$COUNT koneksi terdaftar (harus ≥ 3)"

echo "========================================="
```

**Output target (semua OK):**

```
=== Verifikasi Lingkungan Lab Modul 7 ===
Airflow scheduler          : BERJALAN
Airflow webserver          : BERJALAN
Airflow UI (HTTP)          : OK (HTTP 200)
Atlas REST API             : OK (HTTP 200)
HDFS NameNode              : Live datanodes (1):
spark-submit               : DITEMUKAN (Welcome to Spark 3.5.5)
HiveServer2 (10000)        : BERJALAN
Koneksi Airflow            : 3 koneksi terdaftar (harus ≥ 3)
=========================================
```

---

## 9. Persiapan Data dan Inisialisasi untuk Latihan

### Data yang Digunakan Modul 7

Modul 7 menggunakan **data sintetis yang di-generate secara programatik** oleh helper script, bukan file statis yang disiapkan sebelumnya. Data mengalir melalui pipeline yang dijalankan oleh Airflow:

| Tahap | Source | Format | Path HDFS | Dibuat Oleh |
|---|---|---|---|---|
| Ingest | Script generator | CSV | `/datalake/bronze/latihan/` | `generate_data.py` via DAG |
| Transform | HDFS Bronze | Parquet | `/datalake/silver/latihan/` | `latihan_etl.py` via Spark |
| Serve | HDFS Silver | Parquet | `/datalake/gold/latihan/` | `pipeline_gold.py` via Spark |
| Metadata | Semua tabel | Atlas Entities | Atlas DB (BerkeleyDB) | `daftar_entitas.py` |

### Schema Data Transaksi

```
transaksi_{YYYYMMDD}.csv
├── id        : string   (misal: T001, T002)
├── nilai     : integer  (nilai transaksi dalam Rupiah)
└── kategori  : string   (elektronik, fashion, makanan, dll.)
```

---

### Inisialisasi Data — Step by Step

#### Step 1 — Buat struktur direktori HDFS

```bash
hdfs dfs -mkdir -p /datalake/bronze/latihan
hdfs dfs -mkdir -p /datalake/silver/latihan
hdfs dfs -mkdir -p /datalake/gold/latihan

# Verifikasi
hdfs dfs -ls /datalake/
```

#### Step 2 — Buat script generator data CSV

```bash
cat > /opt/scripts/generate_data.py << 'PYEOF'
#!/usr/bin/env python3
"""
generate_data.py — Generator data transaksi sintetis untuk Modul 7
Penggunaan: python generate_data.py <tanggal YYYY-MM-DD> <jumlah_baris>
Output    : CSV ke stdout dengan header id,nilai,kategori
"""

import sys
import random
import uuid

def main():
    if len(sys.argv) < 3:
        print("Usage: generate_data.py <tanggal> <jumlah_baris>", file=sys.stderr)
        sys.exit(1)

    tanggal = sys.argv[1]           # misal: 2024-03-01
    n       = int(sys.argv[2])      # jumlah baris data

    random.seed(hash(tanggal) % (2**31))  # seed deterministik per tanggal

    KATEGORI = ["elektronik", "fashion", "makanan", "kesehatan", "otomotif"]
    HARGA_BASE = {
        "elektronik": 500_000,
        "fashion":    200_000,
        "makanan":     50_000,
        "kesehatan":  150_000,
        "otomotif":   800_000,
    }

    print("id,nilai,kategori")  # header CSV

    for i in range(1, n + 1):
        kat   = random.choice(KATEGORI)
        nilai = int(HARGA_BASE[kat] * random.uniform(0.5, 3.0))
        tid   = f"T{str(uuid.uuid4())[:6].upper()}"

        # Sisipkan ~3% baris tidak valid (nilai negatif atau id kosong)
        if random.random() < 0.03:
            if random.random() < 0.5:
                tid = ""          # id kosong
            else:
                nilai = -nilai    # nilai negatif

        print(f"{tid},{nilai},{kat}")

if __name__ == "__main__":
    main()
PYEOF

chmod +x /opt/scripts/generate_data.py
echo "[OK] generate_data.py dibuat"
```

#### Step 3 — Buat Spark ETL job (Bronze → Silver)

```bash
cat > /opt/spark-jobs/latihan_etl.py << 'PYEOF'
import sys
from pyspark.sql import SparkSession, functions as F

def main(tanggal: str):
    spark = SparkSession.builder \
        .appName(f"ETL-Latihan-{tanggal}") \
        .master("yarn") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    BRONZE = "hdfs:///datalake/bronze/latihan/"
    SILVER = "hdfs:///datalake/silver/latihan/"

    print(f"[ETL] Memproses data tanggal: {tanggal}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(BRONZE)

    n_raw = df.count()
    print(f"[ETL] Baris raw    : {n_raw}")

    df_clean = df \
        .filter(F.col("id").isNotNull() & (F.col("id") != "")) \
        .filter(F.col("nilai") > 0) \
        .withColumn("nilai", F.col("nilai").cast("double")) \
        .withColumn("kategori", F.upper(F.trim(F.col("kategori")))) \
        .withColumn("tanggal_proses", F.lit(tanggal)) \
        .dropDuplicates(["id"])

    n_clean = df_clean.count()
    print(f"[ETL] Baris valid  : {n_clean}")
    print(f"[ETL] Baris ditolak: {n_raw - n_clean}")

    df_clean.coalesce(2) \
        .write \
        .mode("overwrite") \
        .parquet(SILVER)

    print(f"[ETL] Output ditulis ke: {SILVER}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: latihan_etl.py <tanggal YYYY-MM-DD>")
        sys.exit(1)
    main(sys.argv[1])
PYEOF

echo "[OK] latihan_etl.py dibuat"
```

#### Step 4 — Buat Spark Gold aggregation job (Silver → Gold)

```bash
cat > /opt/spark-jobs/pipeline_gold.py << 'PYEOF'
import sys
from pyspark.sql import SparkSession, functions as F

def main(tanggal: str):
    spark = SparkSession.builder \
        .appName(f"Gold-Latihan-{tanggal}") \
        .master("yarn") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    SILVER = "hdfs:///datalake/silver/latihan/"
    GOLD   = "hdfs:///datalake/gold/latihan/"

    print(f"[GOLD] Agregasi Silver → Gold untuk tanggal: {tanggal}")

    df = spark.read.parquet(SILVER)
    n  = df.count()
    print(f"[GOLD] Baris Silver dibaca: {n}")

    df_agg = df.groupBy("kategori", "tanggal_proses") \
        .agg(
            F.count("*").alias("jumlah_transaksi"),
            F.sum("nilai").alias("total_nilai"),
            F.avg("nilai").alias("rata_nilai"),
            F.min("nilai").alias("min_nilai"),
            F.max("nilai").alias("max_nilai"),
        )

    df_agg.coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(GOLD)

    print(f"[GOLD] Agregasi per kategori:")
    df_agg.orderBy(F.col("total_nilai").desc()).show(truncate=False)
    print(f"[GOLD] Output ditulis ke: {GOLD}")
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pipeline_gold.py <tanggal YYYY-MM-DD>")
        sys.exit(1)
    main(sys.argv[1])
PYEOF

echo "[OK] pipeline_gold.py dibuat"
```

#### Step 5 — Generate data awal dan jalankan pipeline manual

```bash
TANGGAL="2024-03-01"
TANGGAL_NODASH="20240301"

# Generate CSV ke /tmp
python /opt/scripts/generate_data.py $TANGGAL 100 \
  > /tmp/transaksi_${TANGGAL_NODASH}.csv

# Cek isi CSV
head -5 /tmp/transaksi_${TANGGAL_NODASH}.csv
echo "Jumlah baris: $(wc -l < /tmp/transaksi_${TANGGAL_NODASH}.csv)"

# Ingest ke HDFS Bronze
hdfs dfs -put -f /tmp/transaksi_${TANGGAL_NODASH}.csv \
  /datalake/bronze/latihan/

# Verifikasi di HDFS
hdfs dfs -ls /datalake/bronze/latihan/
```

Output yang diharapkan:

```
id,nilai,kategori
TA3B2F,725000,elektronik
TB1C4D,132000,fashion
...
Jumlah baris: 101   (100 data + 1 header)
```

#### Step 6 — Jalankan Spark ETL manual untuk verifikasi pipeline

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  --conf spark.sql.shuffle.partitions=10 \
  /opt/spark-jobs/latihan_etl.py $TANGGAL
```

Verifikasi Silver layer:

```bash
hdfs dfs -ls /datalake/silver/latihan/
hdfs dfs -du -h /datalake/silver/latihan/
```

Jalankan agregasi Gold:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  /opt/spark-jobs/pipeline_gold.py $TANGGAL
```

Verifikasi Gold layer:

```bash
hdfs dfs -ls /datalake/gold/latihan/
```

#### Step 7 — Letakkan DAG pertama (Tahap 1) ke direktori Airflow

```bash
cat > ~/airflow/dags/latihan_pipeline.py << 'DAGEOF'
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "mahasiswa",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def validasi_data(**context):
    tanggal = context["ds_nodash"]
    path = f"/tmp/transaksi_{tanggal}.csv"
    if not os.path.exists(path):
        raise FileNotFoundError(f"File tidak ditemukan: {path}")
    with open(path) as f:
        baris = f.readlines()
    jumlah = len(baris) - 1
    print(f"[VALIDASI] Ditemukan {jumlah} baris data.")
    context["ti"].xcom_push(key="jumlah_baris", value=jumlah)

def cetak_laporan(**context):
    jumlah = context["ti"].xcom_pull(
        task_ids="validasi_data", key="jumlah_baris"
    )
    print(f"[LAPORAN] Pipeline selesai. Total baris: {jumlah}")

with DAG(
    dag_id="latihan_pipeline_transaksi",
    default_args=default_args,
    description="DAG latihan modul 7 - pipeline sederhana",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["latihan", "modul7"],
) as dag:

    mulai = EmptyOperator(task_id="mulai")

    buat_file = BashOperator(
        task_id="buat_file_simulasi",
        bash_command=(
            "python /opt/scripts/generate_data.py "
            "{{ ds }} 100 "
            "> /tmp/transaksi_{{ ds_nodash }}.csv "
            "&& echo 'File berhasil dibuat: '$(wc -l < /tmp/transaksi_{{ ds_nodash }}.csv)' baris'"
        ),
    )

    validasi = PythonOperator(
        task_id="validasi_data",
        python_callable=validasi_data,
        provide_context=True,
    )

    ingest_hdfs = BashOperator(
        task_id="ingest_ke_hdfs",
        bash_command=(
            "hdfs dfs -mkdir -p /datalake/bronze/latihan/ && "
            "hdfs dfs -put -f /tmp/transaksi_{{ ds_nodash }}.csv "
            "/datalake/bronze/latihan/ && "
            "echo 'Ingest selesai. File di HDFS:' && "
            "hdfs dfs -ls /datalake/bronze/latihan/"
        ),
    )

    laporan = PythonOperator(
        task_id="cetak_laporan",
        python_callable=cetak_laporan,
        provide_context=True,
    )

    selesai = EmptyOperator(task_id="selesai")

    mulai >> buat_file >> validasi >> ingest_hdfs >> laporan >> selesai
DAGEOF

echo "[OK] DAG latihan_pipeline.py ditempatkan di ~/airflow/dags/"

# Verifikasi Airflow mendeteksi DAG
sleep 5
airflow dags list | grep latihan
```

#### Step 8 — Verifikasi DAG terdeteksi di Airflow UI

```bash
# Cek via CLI
airflow dags list | grep modul7

# Trigger DAG secara manual untuk test
airflow dags trigger latihan_pipeline_transaksi

# Pantau status
sleep 10
airflow dags state latihan_pipeline_transaksi \
  $(airflow dags list-runs latihan_pipeline_transaksi \
    --output json 2>/dev/null | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['run_id'])" 2>/dev/null)
```

---

## 10. Checklist Sebelum Memulai Latihan

**Airflow:**
- [ ] `pgrep -f "airflow scheduler"` mengembalikan PID
- [ ] `pgrep -f "airflow webserver"` mengembalikan PID
- [ ] `curl -s http://localhost:8080/health` mengembalikan HTTP 200
- [ ] Login di `http://localhost:8080` berhasil dengan `admin / admin123`
- [ ] DAG `latihan_pipeline_transaksi` terlihat di halaman DAGs

**Apache Atlas:**
- [ ] `curl -u admin:admin http://localhost:21000/api/atlas/admin/status` mengembalikan HTTP 200
- [ ] Login di `http://localhost:21000` berhasil dengan `admin / admin`
- [ ] Halaman Search dapat diakses

**Hadoop + Spark:**
- [ ] `jps` menampilkan NameNode, DataNode, ResourceManager, NodeManager
- [ ] `hdfs dfs -ls /datalake/` menampilkan bronze, silver, gold
- [ ] `hdfs dfs -ls /datalake/bronze/latihan/` menampilkan file CSV
- [ ] `hdfs dfs -ls /datalake/silver/latihan/` menampilkan file Parquet
- [ ] `spark-submit --version` tidak error

**Koneksi Airflow:**
- [ ] `airflow connections list | grep spark` menampilkan `spark_default`
- [ ] `airflow connections list | grep hive` menampilkan `hive_default` dan `hiveserver2_default`

**Scripts:**
- [ ] `/opt/scripts/generate_data.py` ada dan dapat dijalankan
- [ ] `/opt/spark-jobs/latihan_etl.py` ada
- [ ] `/opt/spark-jobs/pipeline_gold.py` ada

Jika semua centang terpenuhi, lingkungan lab **siap digunakan** untuk seluruh latihan Modul 7.

---

## Ringkasan Port dan Layanan

| Port | Layanan | URL | Kredensial |
|---|---|---|---|
| 8080 | Apache Airflow Web UI | `http://localhost:8080` | admin / admin123 |
| 21000 | Apache Atlas Web UI | `http://localhost:21000` | admin / admin |
| 9870 | HDFS Web UI | `http://localhost:9870` | — |
| 8088 | YARN ResourceManager | `http://localhost:8088` | — |
| 4040 | Spark UI (saat job aktif) | `http://localhost:4040` | — |
