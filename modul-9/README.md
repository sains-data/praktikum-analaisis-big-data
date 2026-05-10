# Panduan Setup Lab Modul 9 — Machine Learning Big Data
> Apache Spark MLlib + Hadoop HDFS + YARN · Single-Node Pseudo-Distributed · Docker

> **Catatan:** Modul 9 menggunakan **lingkungan yang sama persis** dengan Modul 5
> (repositori `bigdata-spark`). Jika kontainer sudah pernah dibuat dari Modul 5,
> lewati langkah build dan langsung ke [Langkah 5 — Menjalankan Kontainer](#5-menjalankan-kontainer).

---

## Daftar Isi

1. [Prasyarat](#1-prasyarat)
2. [Struktur Folder](#2-struktur-folder)
3. [Membuat Semua File Konfigurasi](#3-membuat-semua-file-konfigurasi)
4. [Mengunduh Dependensi Binary](#4-mengunduh-dependensi-binary)
5. [Membangun Docker Image](#5-membangun-docker-image)
6. [Menjalankan Kontainer](#6-menjalankan-kontainer)
7. [Verifikasi Layanan Hadoop & HDFS](#7-verifikasi-layanan-hadoop--hdfs)
8. [Mengakses Hadoop Web UI dan YARN UI](#8-mengakses-hadoop-web-ui-dan-yarn-ui)
9. [Mengakses Spark UI](#9-mengakses-spark-ui)
10. [Persiapan Data & Inisialisasi untuk Latihan](#10-persiapan-data--inisialisasi-untuk-latihan)
11. [Checklist Sebelum Memulai Latihan](#11-checklist-sebelum-memulai-latihan)

---

## 1. Prasyarat

Pastikan seluruh perangkat lunak berikut sudah terpasang sebelum memulai.

| Perangkat Lunak | Versi Minimum | Cara Cek |
|---|---|---|
| Docker Desktop | 4.25 / Engine 24.0 | `docker --version` |
| Docker Compose | 2.20 | `docker compose version` |
| Git | 2.x | `git --version` |
| WSL2 + Ubuntu 22.04 | — | `wsl --list --verbose` |
| RAM tersedia | 8 GB | Task Manager / `free -h` |

> **Windows:** Semua perintah bash dijalankan dari dalam terminal **WSL Ubuntu**,
> bukan PowerShell. Pastikan Docker Desktop sudah diintegrasikan dengan WSL2
> (Settings → Resources → WSL Integration → aktifkan Ubuntu-22.04).

**Instalasi WSL2 (jika belum ada) — jalankan dari PowerShell Administrator:**

```powershell
wsl --install -d Ubuntu-22.04
```

Setelah selesai, buka terminal Ubuntu dan arahkan ke drive C:

```bash
cd /mnt/c
```

---

## 2. Struktur Folder

Berikut struktur folder lengkap repositori yang akan digunakan:

```
bigdata-spark/                        ← root repositori
├── Dockerfile                        ← image Hadoop + Spark
├── build.sh                          ← script build image
├── start.sh                          ← script jalankan kontainer
├── stop.sh                           ← script hentikan kontainer
├── login.sh                          ← script masuk ke kontainer
├── docker-compose.yml                ← definisi service kontainer
├── config/
│   ├── hadoop/
│   │   ├── core-site.xml             ← konfigurasi HDFS URI
│   │   ├── hdfs-site.xml             ← konfigurasi replikasi HDFS
│   │   ├── mapred-site.xml           ← konfigurasi MapReduce
│   │   └── yarn-site.xml             ← konfigurasi YARN
│   └── spark/
│       └── spark-defaults.conf       ← konfigurasi Spark default
├── scripts/
│   └── bootstrap.sh                  ← script inisialisasi saat kontainer start
├── modul9/                           ← direktori khusus Modul 9
│   ├── data/
│   │   ├── transaksi_ml.json         ← dataset utama (10.000 baris)
│   │   ├── referensi_schema.json     ← dokumentasi schema kolom
│   │   └── README_DATA.md            ← keterangan file data
│   └── scripts/
│       ├── buat_data_ml.py           ← generator dataset ke HDFS
│       ├── linear_regression.py      ← skrip Tahap 2a
│       ├── klasifikasi_dt.py         ← skrip Tahap 2b
│       ├── kmeans_elbow.py           ← skrip Tahap 3
│       └── pipeline_ml_e2e.py        ← skrip Tahap 4
│
├── hadoop-3.4.1.tar.gz               ← ⬅ diunduh manual (lihat Langkah 4)
└── spark-3.5.5-bin-hadoop3.tgz       ← ⬅ diunduh manual (lihat Langkah 4)
```

Buat direktori kerja dari terminal Ubuntu:

```bash
# Dari /mnt/c atau lokasi pilihan Anda
git clone https://github.com/sains-data/bigdata-spark.git
cd bigdata-spark

# Buat direktori Modul 9
mkdir -p modul9/data modul9/scripts
```

> **Catatan line ending:** Setelah clone, pastikan semua file `.sh` menggunakan
> format LF, bukan CRLF. Jalankan perintah berikut jika perlu:
> ```bash
> find . -name "*.sh" -exec sed -i 's/\r//' {} \;
> ```

---

## 3. Membuat Semua File Konfigurasi

Jalankan perintah berikut satu per satu dari dalam direktori `bigdata-spark/`.

### 3.1 `Dockerfile`

```bash
cat > Dockerfile << 'EOF'
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# Install dependensi sistem
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    openssh-server \
    openssh-client \
    python3 \
    python3-pip \
    wget \
    curl \
    net-tools \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Setup SSH tanpa password (diperlukan Hadoop)
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config

# Install PySpark dan library Python untuk Modul 9
RUN pip3 install --no-cache-dir pyspark==3.5.5

# Salin dan ekstrak Hadoop
COPY hadoop-3.4.1.tar.gz /tmp/
RUN tar -xzf /tmp/hadoop-3.4.1.tar.gz -C /opt/ && \
    mv /opt/hadoop-3.4.1 /opt/hadoop && \
    rm /tmp/hadoop-3.4.1.tar.gz

# Salin dan ekstrak Spark
COPY spark-3.5.5-bin-hadoop3.tgz /tmp/
RUN tar -xzf /tmp/spark-3.5.5-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-3.5.5-bin-hadoop3 /opt/spark && \
    rm /tmp/spark-3.5.5-bin-hadoop3.tgz

# Konfigurasi Hadoop
COPY config/hadoop/core-site.xml    $HADOOP_HOME/etc/hadoop/
COPY config/hadoop/hdfs-site.xml    $HADOOP_HOME/etc/hadoop/
COPY config/hadoop/mapred-site.xml  $HADOOP_HOME/etc/hadoop/
COPY config/hadoop/yarn-site.xml    $HADOOP_HOME/etc/hadoop/

# Set JAVA_HOME di hadoop-env.sh
RUN echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

# Konfigurasi Spark
COPY config/spark/spark-defaults.conf $SPARK_HOME/conf/

# Bootstrap script
COPY scripts/bootstrap.sh /bootstrap.sh
RUN chmod +x /bootstrap.sh

# Format HDFS NameNode
RUN $HADOOP_HOME/bin/hdfs namenode -format -force

EXPOSE 9870 8088 4040 9000 8032

CMD ["/bootstrap.sh"]
EOF
```

---

### 3.2 `config/hadoop/core-site.xml`

```bash
mkdir -p config/hadoop config/spark

cat > config/hadoop/core-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://localhost:9000</value>
    <description>URI default HDFS NameNode</description>
  </property>
</configuration>
EOF
```

### 3.3 `config/hadoop/hdfs-site.xml`

```bash
cat > config/hadoop/hdfs-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
    <description>Replikasi = 1 untuk single-node cluster</description>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/opt/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/opt/hdfs/datanode</value>
  </property>
  <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
  </property>
</configuration>
EOF
```

### 3.4 `config/hadoop/mapred-site.xml`

```bash
cat > config/hadoop/mapred-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
</configuration>
EOF
```

### 3.5 `config/hadoop/yarn-site.xml`

```bash
cat > config/hadoop/yarn-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.env-whitelist</name>
    <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
  </property>
  <!-- Batasan resource untuk single-node (sesuaikan dengan RAM tersedia) -->
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>4096</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>2048</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>2</value>
  </property>
</configuration>
EOF
```

### 3.6 `config/spark/spark-defaults.conf`

```bash
cat > config/spark/spark-defaults.conf << 'EOF'
# Master: gunakan YARN
spark.master                     yarn
spark.submit.deployMode          client

# Driver dan executor memory untuk single-node
spark.driver.memory              1g
spark.executor.memory            512m
spark.executor.cores             1

# Kurangi shuffle partition untuk dataset kecil (<100K baris)
spark.sql.shuffle.partitions     20

# Event log untuk Spark History Server
spark.eventLog.enabled           true
spark.eventLog.dir               hdfs:///spark-logs

# Adaptive Query Execution
spark.sql.adaptive.enabled       true
EOF
```

### 3.7 `scripts/bootstrap.sh`

```bash
mkdir -p scripts

cat > scripts/bootstrap.sh << 'EOF'
#!/bin/bash
# Bootstrap: dijalankan otomatis saat kontainer start
set -e

LOG=/tmp/bootstrap.log
echo "[$(date)] Bootstrap dimulai..." | tee $LOG

# Start SSH daemon (diperlukan Hadoop)
service ssh start >> $LOG 2>&1
echo "[$(date)] SSH aktif" | tee -a $LOG

# Buat direktori HDFS storage
mkdir -p /opt/hdfs/namenode /opt/hdfs/datanode

# Start HDFS
$HADOOP_HOME/sbin/start-dfs.sh >> $LOG 2>&1
echo "[$(date)] HDFS aktif" | tee -a $LOG

# Start YARN
$HADOOP_HOME/sbin/start-yarn.sh >> $LOG 2>&1
echo "[$(date)] YARN aktif" | tee -a $LOG

# Buat direktori Spark event log di HDFS
hdfs dfs -mkdir -p /spark-logs >> $LOG 2>&1

# Buat struktur direktori Data Lake untuk Modul 9
hdfs dfs -mkdir -p /datalake/bronze/transaksi >> $LOG 2>&1
hdfs dfs -mkdir -p /datalake/silver/transaksi >> $LOG 2>&1
hdfs dfs -mkdir -p /datalake/gold/prediksi_risiko >> $LOG 2>&1
hdfs dfs -mkdir -p /datalake/gold/prediksi_segmen >> $LOG 2>&1
hdfs dfs -mkdir -p /datalake/gold/segmentasi_pelanggan >> $LOG 2>&1
hdfs dfs -mkdir -p /feature_store/pelanggan_v1 >> $LOG 2>&1
hdfs dfs -mkdir -p /models/segmentasi_dt/v1 >> $LOG 2>&1
hdfs dfs -mkdir -p /models/klasifikasi_transaksi/v1 >> $LOG 2>&1

echo "[$(date)] Semua direktori HDFS siap" | tee -a $LOG
echo "[$(date)] Bootstrap selesai. Sistem siap." | tee -a $LOG

# Jaga kontainer tetap hidup
tail -f /dev/null
EOF
chmod +x scripts/bootstrap.sh
```

### 3.8 `docker-compose.yml`

```bash
cat > docker-compose.yml << 'EOF'
services:
  bigdata-spark:
    build: .
    container_name: bigdata-spark
    hostname: localhost
    ports:
      - "9870:9870"   # HDFS Web UI
      - "8088:8088"   # YARN ResourceManager UI
      - "4040:4040"   # Spark UI (aktif saat job berjalan)
      - "9000:9000"   # HDFS NameNode RPC
      - "8032:8032"   # YARN Scheduler
    volumes:
      - bigdata-hdfs:/opt/hdfs
      - ./modul9:/modul9        # mount direktori Modul 9
    mem_limit: 6g
    restart: unless-stopped

volumes:
  bigdata-hdfs:
    driver: local
EOF
```

### 3.9 `build.sh`

```bash
cat > build.sh << 'EOF'
#!/bin/bash
echo "=== Membangun Docker image bigdata-spark ==="
echo "Pastikan hadoop-3.4.1.tar.gz dan spark-3.5.5-bin-hadoop3.tgz"
echo "sudah ada di direktori ini sebelum melanjutkan."
echo ""

# Cek keberadaan file binary
if [ ! -f hadoop-3.4.1.tar.gz ]; then
    echo "[ERROR] hadoop-3.4.1.tar.gz tidak ditemukan!"
    echo "Unduh dari: https://downloads.apache.org/hadoop/common/hadoop-3.4.1/"
    exit 1
fi

if [ ! -f spark-3.5.5-bin-hadoop3.tgz ]; then
    echo "[ERROR] spark-3.5.5-bin-hadoop3.tgz tidak ditemukan!"
    echo "Unduh dari: https://archive.apache.org/dist/spark/spark-3.5.5/"
    exit 1
fi

docker compose build
echo ""
echo "[OK] Build selesai. Jalankan: bash start.sh"
EOF
chmod +x build.sh
```

### 3.10 `start.sh`

```bash
cat > start.sh << 'EOF'
#!/bin/bash
echo "=== Menjalankan kontainer bigdata-spark ==="
docker compose up -d
echo ""
echo "Tunggu 30-60 detik hingga Hadoop dan YARN aktif sepenuhnya."
echo "Cek log: docker exec bigdata-spark cat /tmp/bootstrap.log"
echo ""
echo "UI yang tersedia setelah siap:"
echo "  HDFS Web UI  : http://localhost:9870"
echo "  YARN UI      : http://localhost:8088"
echo "  Spark UI     : http://localhost:4040 (aktif saat job berjalan)"
EOF
chmod +x start.sh
```

### 3.11 `stop.sh`

```bash
cat > stop.sh << 'EOF'
#!/bin/bash
echo "=== Menghentikan kontainer bigdata-spark ==="
docker compose down
echo "[OK] Kontainer dihentikan. Data HDFS tetap tersimpan di volume."
echo "Untuk menghapus data HDFS juga: docker compose down -v"
EOF
chmod +x stop.sh
```

### 3.12 `login.sh`

```bash
cat > login.sh << 'EOF'
#!/bin/bash
echo "=== Masuk ke kontainer bigdata-spark ==="
docker exec -it bigdata-spark bash
EOF
chmod +x login.sh
```

---

## 4. Mengunduh Dependensi Binary

Kedua file berikut harus diunduh **manual** karena ukurannya terlalu besar untuk
disertakan di repositori. Letakkan keduanya di **root direktori** `bigdata-spark/`.

### File 1 — Hadoop 3.4.1

```bash
# Dari dalam WSL Ubuntu, jalankan:
wget https://downloads.apache.org/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz

# Jika wget lambat, gunakan mirror:
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/hadoop-3.4.1.tar.gz
```

### File 2 — Apache Spark 3.5.5 (build Hadoop 3)

```bash
wget https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
```

Verifikasi kedua file sudah ada:

```bash
ls -lh hadoop-3.4.1.tar.gz spark-3.5.5-bin-hadoop3.tgz
```

Output yang diharapkan:

```
-rw-r--r-- 1 user user 706M  hadoop-3.4.1.tar.gz
-rw-r--r-- 1 user user 400M  spark-3.5.5-bin-hadoop3.tgz
```

> **Alternatif unduh manual:** Buka browser di Windows, unduh dari tautan di atas,
> lalu salin ke direktori WSL:
> ```bash
> cp /mnt/c/Users/<NamaUser>/Downloads/hadoop-3.4.1.tar.gz .
> cp /mnt/c/Users/<NamaUser>/Downloads/spark-3.5.5-bin-hadoop3.tgz .
> ```

---

## 5. Membangun Docker Image

```bash
bash build.sh
```

Proses ini membutuhkan **10–20 menit** pada unduhan pertama karena mengekstrak
Hadoop (~700 MB) dan Spark (~400 MB). Proses build berikutnya jauh lebih cepat
berkat cache Docker.

Output akhir yang diharapkan:

```
Successfully built a1b2c3d4e5f6
Successfully tagged bigdata-spark-bigdata-spark:latest
[OK] Build selesai. Jalankan: bash start.sh
```

> Jika build gagal dengan error `COPY hadoop-3.4.1.tar.gz`, pastikan kedua file
> binary sudah ada di direktori root `bigdata-spark/` (bukan di subdirektori).

---

## 6. Menjalankan Kontainer

### Langkah 6.1 — Start kontainer

```bash
bash start.sh
```

### Langkah 6.2 — Cek status kontainer

```bash
docker ps
```

Output yang diharapkan:

```
CONTAINER ID   IMAGE                    STATUS          PORTS
a1b2c3d4e5f6   bigdata-spark-bigdata-   Up 2 minutes    0.0.0.0:9870->9870/tcp, ...
```

### Langkah 6.3 — Pantau log bootstrap

Tunggu sekitar 30–60 detik, lalu cek log inisialisasi:

```bash
docker exec bigdata-spark cat /tmp/bootstrap.log
```

Output yang menandakan sistem siap:

```
[Mon Apr 15 08:01:00 UTC 2024] Bootstrap dimulai...
[Mon Apr 15 08:01:02 UTC 2024] SSH aktif
[Mon Apr 15 08:01:15 UTC 2024] HDFS aktif
[Mon Apr 15 08:01:25 UTC 2024] YARN aktif
[Mon Apr 15 08:01:26 UTC 2024] Semua direktori HDFS siap
[Mon Apr 15 08:01:26 UTC 2024] Bootstrap selesai. Sistem siap.
```

### Langkah 6.4 — Masuk ke dalam kontainer

```bash
bash login.sh
```

Anda kini berada di dalam shell kontainer sebagai `root`.

---

## 7. Verifikasi Layanan Hadoop & HDFS

Semua perintah berikut dijalankan **di dalam kontainer** (setelah `bash login.sh`).

### Langkah 7.1 — Cek proses Java yang berjalan

```bash
jps
```

Pastikan **semua empat proses** ini muncul:

```
1234 NameNode
1456 DataNode
1678 ResourceManager
1890 NodeManager
2100 Jps
```

Jika ada yang tidak muncul, jalankan:

```bash
start-dfs.sh && start-yarn.sh
```

### Langkah 7.2 — Cek status HDFS

```bash
hdfs dfsadmin -report
```

Perhatikan bagian `Live datanodes (1)` — harus ada tepat 1 datanode aktif.

### Langkah 7.3 — Verifikasi struktur direktori HDFS untuk Modul 9

```bash
hdfs dfs -ls /datalake/
```

Output yang diharapkan:

```
drwxr-xr-x  - root supergroup  /datalake/bronze
drwxr-xr-x  - root supergroup  /datalake/gold
drwxr-xr-x  - root supergroup  /datalake/silver
```

```bash
hdfs dfs -ls /datalake/bronze/
hdfs dfs -ls /models/
```

### Langkah 7.4 — Uji tulis dan baca HDFS

```bash
# Buat file uji
echo "test hdfs modul9" > /tmp/test.txt

# Upload ke HDFS
hdfs dfs -put /tmp/test.txt /

# Baca kembali
hdfs dfs -cat /test.txt

# Bersihkan
hdfs dfs -rm /test.txt
```

---

## 8. Mengakses Hadoop Web UI dan YARN UI

Buka browser di komputer host (Windows/Mac), **bukan di dalam kontainer**.

### HDFS Web UI

```
http://localhost:9870
```

Yang bisa dilihat:

| Menu | Informasi |
|---|---|
| **Overview** | Status cluster, kapasitas HDFS, live nodes |
| **Datanodes** | Status 1 datanode aktif |
| **Utilities → Browse the file system** | Menjelajah isi direktori HDFS, termasuk `/datalake/` |
| **Utilities → Logs** | Log NameNode |

> Pastikan terlihat **1 Live Node** dan **0 Dead Nodes** di halaman Overview.

### YARN ResourceManager UI

```
http://localhost:8088
```

Yang bisa dilihat:

| Menu | Informasi |
|---|---|
| **Cluster → Nodes** | NodeManager aktif |
| **Applications** | Daftar Spark job yang sedang berjalan atau sudah selesai |
| **Scheduler** | Kapasitas resource yang tersedia |

> Di sinilah semua Spark job via `spark-submit --master yarn` akan terdaftar dan
> bisa dipantau statusnya.

---

## 9. Mengakses Spark UI

Spark UI **hanya aktif saat ada Spark job yang sedang berjalan**. Tersedia di:

```
http://localhost:4040
```

### Langkah 9.1 — Jalankan PySpark shell untuk membuka Spark UI

Di dalam kontainer:

```bash
pyspark --master yarn --executor-memory 512m --num-executors 2
```

Setelah prompt `>>>` muncul, buka browser ke `http://localhost:4040`.

### Langkah 9.2 — Navigasi Spark UI

| Tab | Yang Diamati di Modul 9 |
|---|---|
| **Jobs** | Setiap pemanggilan `.fit()`, `.transform()`, `.count()` membuat satu job |
| **Stages** | Breakdown stage per job — cari stage dengan shuffle paling besar |
| **Storage** | DataFrame yang di-cache (setelah `.cache()`) |
| **SQL / DataFrame** | Visualisasi query plan — terlihat VectorAssembler, StandardScaler |
| **Environment** | Konfigurasi Spark aktif |

### Langkah 9.3 — Jalankan test job sederhana

Di dalam shell PySpark:

```python
# Test: buat DataFrame kecil dan lihat di Spark UI tab Jobs
df = spark.range(1000).toDF("id")
df.groupBy((df.id % 10).alias("grup")).count().show()
```

Job ini akan muncul di Spark UI tab **Jobs**. Klik job tersebut untuk melihat detail stage-nya.

Keluar dari PySpark shell:

```python
exit()
```

---

## 10. Persiapan Data & Inisialisasi untuk Latihan

### Data yang Digunakan Modul 9

Modul 9 menggunakan **satu dataset utama** yang di-generate secara programatik
ke dalam HDFS, bukan file statis yang diupload. Berikut ringkasannya:

| Layer HDFS | Path | Isi | Digunakan di Tahap |
|---|---|---|---|
| **Bronze** | `/datalake/bronze/transaksi/` | 10.000 baris raw — format Parquet | Tahap 1 (sumber awal) |
| **Silver** | `/datalake/silver/transaksi/` | 10.000 baris (sama, tanpa transformasi besar) | Tahap 2, 3, 4 (input semua model ML) |
| **Gold** | `/datalake/gold/prediksi_segmen/` | Hasil prediksi Decision Tree | Tahap 4 (output model) |
| **Gold** | `/datalake/gold/segmentasi_pelanggan/` | Hasil K-Means clustering | Tahap 3 (output clustering) |
| `/models/` | `/models/segmentasi_dt/v1/` | PipelineModel tersimpan | Tahap 4 (model registry) |

### Schema Dataset

| Kolom | Tipe | Deskripsi | Contoh Nilai |
|---|---|---|---|
| `id_transaksi` | string | ID unik transaksi (UUID 8 char) | `"1a2b3c4d"` |
| `id_pelanggan` | string | ID pelanggan (200 unik) | `"usr-0042"` |
| `kategori` | string | Kategori produk (6 nilai) | `"elektronik"` |
| `channel` | string | Saluran transaksi (4 nilai) | `"mobile"` |
| `kuantitas` | integer | Jumlah unit (1–20) | `5` |
| `harga_satuan` | double | Harga per unit dalam Rupiah | `1250000.00` |
| `diskon` | double | Diskon (0.0–0.3) | `0.15` |
| `total_nilai` | double | Nilai akhir transaksi | `5312500.00` |
| `berat_kg` | double | Berat paket dalam kg (0.1–10.0) | `2.35` |

Nilai `total_nilai` dihitung sebagai: `kuantitas × harga_satuan × (1 - diskon)`.
Harga satuan berbeda per kategori sehingga menghasilkan distribusi yang realistis
dan bisa membentuk segmen yang bermakna.

---

### Inisialisasi Data — Step by Step

#### Step 1 — Pastikan kontainer berjalan dan semua layanan aktif

```bash
# Dari luar kontainer (terminal WSL)
docker ps

# Cek log bootstrap
docker exec bigdata-spark cat /tmp/bootstrap.log | tail -5
```

Pastikan baris terakhir adalah `Bootstrap selesai. Sistem siap.`

#### Step 2 — Masuk ke kontainer

```bash
bash login.sh
```

#### Step 3 — Salin script generator ke kontainer

Script generator sudah ter-mount melalui volume Docker di path `/modul9/scripts/`.
Salin ke `/tmp/` agar mudah diakses:

```bash
cp /modul9/scripts/buat_data_ml.py /tmp/buat_data_ml.py
```

Jika direktori `/modul9/` belum ter-mount, buat script langsung di dalam kontainer:

```bash
cat > /tmp/buat_data_ml.py << 'PYEOF'
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)
import random, uuid

spark = SparkSession.builder \
    .appName("BuatDataML") \
    .master("yarn") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KATEGORI = ["elektronik", "fashion", "makanan",
            "kesehatan", "otomotif", "olahraga"]
CHANNEL  = ["mobile", "web", "atm", "teller"]
N        = 10_000

random.seed(42)
rows = []
for i in range(N):
    kat     = random.choice(KATEGORI)
    channel = random.choice(CHANNEL)
    kuantitas = random.randint(1, 20)
    base = {
        "elektronik": 500_000, "otomotif": 800_000,
        "fashion":    200_000, "makanan":   50_000,
        "kesehatan":  150_000, "olahraga": 300_000,
    }[kat]
    harga  = base * random.uniform(0.5, 3.0)
    diskon = random.uniform(0, 0.3)
    total  = kuantitas * harga * (1 - diskon)
    berat  = round(random.uniform(0.1, 10.0), 2)
    rows.append((
        str(uuid.uuid4())[:8],
        f"usr-{random.randint(1, 200):04d}",
        kat, channel,
        kuantitas, round(harga, 2),
        round(diskon, 3), round(total, 2), berat
    ))

schema = StructType([
    StructField("id_transaksi",  StringType(),  True),
    StructField("id_pelanggan",  StringType(),  True),
    StructField("kategori",      StringType(),  True),
    StructField("channel",       StringType(),  True),
    StructField("kuantitas",     IntegerType(), True),
    StructField("harga_satuan",  DoubleType(),  True),
    StructField("diskon",        DoubleType(),  True),
    StructField("total_nilai",   DoubleType(),  True),
    StructField("berat_kg",      DoubleType(),  True),
])

df = spark.createDataFrame(rows, schema=schema)

# Simpan ke Bronze
df.write.mode("overwrite").parquet("hdfs:///datalake/bronze/transaksi/")

# Simpan ke Silver (siap untuk semua skrip ML)
df.write.mode("overwrite").parquet("hdfs:///datalake/silver/transaksi/")

print(f"\n[OK] Dataset dibuat: {df.count()} baris")
df.printSchema()
df.show(5)
spark.stop()
PYEOF
```

#### Step 4 — Jalankan generator dataset

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  /tmp/buat_data_ml.py
```

Proses ini membutuhkan sekitar **2–5 menit**. Output akhir yang diharapkan:

```
[OK] Dataset dibuat: 10000 baris
root
 |-- id_transaksi: string (nullable = true)
 |-- id_pelanggan: string (nullable = true)
 |-- kategori: string (nullable = true)
 |-- channel: string (nullable = true)
 |-- kuantitas: integer (nullable = true)
 |-- harga_satuan: double (nullable = true)
 |-- diskon: double (nullable = true)
 |-- total_nilai: double (nullable = true)
 |-- berat_kg: double (nullable = true)

+------------+------------+-----------+-------+---------+------------+------+------------------+--------+
|id_transaksi|id_pelanggan|kategori   |channel|kuantitas|harga_satuan|diskon|total_nilai       |berat_kg|
+------------+------------+-----------+-------+---------+------------+------+------------------+--------+
|1a2b3c4d    |usr-0042    |elektronik |mobile |5        |1250000.0   |0.15  |5312500.0         |2.35    |
...
```

#### Step 5 — Verifikasi data tersedia di HDFS

```bash
# Cek file Parquet tersimpan
hdfs dfs -ls /datalake/silver/transaksi/

# Cek ukuran data
hdfs dfs -du -h /datalake/silver/transaksi/

# Cek jumlah file partisi
hdfs dfs -ls /datalake/silver/transaksi/ | wc -l
```

Output yang diharapkan:

```
# hdfs dfs -du -h /datalake/silver/transaksi/
768.0 K  /datalake/silver/transaksi/
```

#### Step 6 — Eksplorasi awal data via PySpark Shell

```bash
pyspark --master yarn --executor-memory 512m --num-executors 2
```

Di prompt `>>>`, jalankan:

```python
from pyspark.sql import functions as F

df = spark.read.parquet("hdfs:///datalake/silver/transaksi/")

# Verifikasi jumlah baris dan kolom
print(f"Baris : {df.count()}")
print(f"Kolom : {len(df.columns)}")

# Statistik deskriptif kolom numerik
df.select("kuantitas", "harga_satuan", "diskon", "total_nilai").describe().show()

# Distribusi per kategori
df.groupBy("kategori").count().orderBy(F.col("count").desc()).show()

# Cek nilai null
from pyspark.sql.functions import col, count, when
df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
```

Catat hasil eksplorasi pada tabel di Latihan Tahap 1. Ketik `exit()` untuk keluar.

#### Step 7 — Data siap untuk semua tahap latihan

Setelah Step 6 berhasil, semua skrip latihan bisa langsung dijalankan:

```bash
# Tahap 2a — Linear Regression
spark-submit --master yarn --deploy-mode client \
  --executor-memory 512m --num-executors 2 \
  /modul9/scripts/linear_regression.py

# Tahap 2b — Logistic Regression & Decision Tree
spark-submit --master yarn --deploy-mode client \
  --executor-memory 512m --num-executors 2 \
  /modul9/scripts/klasifikasi_dt.py

# Tahap 3 — K-Means Clustering
spark-submit --master yarn --deploy-mode client \
  --executor-memory 512m --num-executors 2 \
  /modul9/scripts/kmeans_elbow.py

# Tahap 4 — Pipeline ML End-to-End
spark-submit --master yarn --deploy-mode client \
  --executor-memory 512m --num-executors 2 \
  --conf spark.sql.shuffle.partitions=20 \
  /modul9/scripts/pipeline_ml_e2e.py
```

---

### Reset Data (untuk percobaan ulang dari nol)

```bash
# Hapus data di HDFS
hdfs dfs -rm -r /datalake/bronze/transaksi
hdfs dfs -rm -r /datalake/silver/transaksi
hdfs dfs -rm -r /datalake/gold/prediksi_segmen
hdfs dfs -rm -r /datalake/gold/segmentasi_pelanggan
hdfs dfs -rm -r /models/segmentasi_dt

# Buat ulang direktori
hdfs dfs -mkdir -p /datalake/bronze/transaksi
hdfs dfs -mkdir -p /datalake/silver/transaksi
hdfs dfs -mkdir -p /datalake/gold/prediksi_segmen
hdfs dfs -mkdir -p /datalake/gold/segmentasi_pelanggan
hdfs dfs -mkdir -p /models/segmentasi_dt/v1

# Generate ulang dataset
spark-submit --master yarn --deploy-mode client \
  --executor-memory 512m --num-executors 2 \
  /tmp/buat_data_ml.py
```

---

## 11. Checklist Sebelum Memulai Latihan

Gunakan checklist ini sebelum membagikan ke mahasiswa.

**Infrastruktur:**
- [ ] `docker ps` menampilkan `bigdata-spark` dengan status **Up**
- [ ] `docker exec bigdata-spark cat /tmp/bootstrap.log` diakhiri `Bootstrap selesai. Sistem siap.`
- [ ] `docker exec bigdata-spark jps` menampilkan NameNode, DataNode, ResourceManager, NodeManager

**HDFS:**
- [ ] `hdfs dfs -ls /datalake/silver/transaksi/` menampilkan file Parquet
- [ ] `hdfs dfs -du -h /datalake/silver/transaksi/` menampilkan ukuran > 0
- [ ] HDFS Web UI terbuka di `http://localhost:9870` dengan **1 Live Node**

**YARN:**
- [ ] YARN UI terbuka di `http://localhost:8088`
- [ ] Tab **Nodes** menampilkan 1 node aktif

**Spark:**
- [ ] `spark-submit --master yarn /tmp/buat_data_ml.py` berhasil (exit code 0)
- [ ] Spark UI terbuka di `http://localhost:4040` saat job berjalan

**Data:**
- [ ] Dataset 10.000 baris tersedia di `/datalake/silver/transaksi/`
- [ ] Direktori Gold dan model sudah tersedia di HDFS
- [ ] Eksplorasi awal via PySpark Shell menampilkan statistik yang masuk akal

Jika semua centang terpenuhi, lingkungan lab **siap digunakan** untuk Latihan Modul 9.

---

## Ringkasan Port

| Port | Layanan | URL | Aktif |
|---|---|---|---|
| 9870 | HDFS Web UI | `http://localhost:9870` | Selalu |
| 8088 | YARN ResourceManager UI | `http://localhost:8088` | Selalu |
| 4040 | Spark UI | `http://localhost:4040` | Hanya saat job berjalan |
| 9000 | HDFS NameNode RPC | — | Internal |
| 8032 | YARN Scheduler | — | Internal |
