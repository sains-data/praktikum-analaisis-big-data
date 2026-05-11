# Panduan Setup Lab Modul 10 — Monitoring, Visualisasi, dan Eksplorasi Big Data
> Apache Superset · PostgreSQL · Prometheus · Node Exporter · Grafana · Docker Compose

> **Catatan Arsitektur:** Modul 10 berdiri **terpisah** dari Modul 8 dan Modul 9.
> Modul ini menjalankan lima layanan baru via Docker Compose. Kontainer Hadoop–Spark
> dari Modul 9 (`bigdata-spark`) tetap dibutuhkan **hanya** pada Langkah 8
> (ekspor data ke PostgreSQL). Semua layanan Modul 10 berjalan di jaringan Docker
> tersendiri (`modul10-net`).

---

## Daftar Isi

1. [Prasyarat](#1-prasyarat)
2. [Struktur Folder](#2-struktur-folder)
3. [Membuat Semua File Konfigurasi](#3-membuat-semua-file-konfigurasi)
4. [Menjalankan Semua Layanan](#4-menjalankan-semua-layanan)
5. [Verifikasi Setiap Layanan](#5-verifikasi-setiap-layanan)
6. [Mengakses Semua UI](#6-mengakses-semua-ui)
7. [Menghubungkan Grafana ke Prometheus](#7-menghubungkan-grafana-ke-prometheus)
8. [Persiapan Data dan Inisialisasi untuk Latihan](#8-persiapan-data-dan-inisialisasi-untuk-latihan)
9. [Checklist Sebelum Memulai Latihan](#9-checklist-sebelum-memulai-latihan)

---

## 1. Prasyarat

| Perangkat Lunak | Versi Minimum | Cara Cek |
|---|---|---|
| Docker Engine | 24.0 | `docker --version` |
| Docker Compose | 2.20 | `docker compose version` |
| Git | 2.x | `git --version` |
| RAM tersedia | 8 GB (10 GB direkomendasikan) | Task Manager / `free -h` |
| Kontainer `bigdata-spark` | Dari Modul 9 | `docker ps` |

> **Catatan RAM:** Modul 10 menjalankan 5 container sekaligus (Superset, PostgreSQL,
> Prometheus, Node Exporter, Grafana). Jika RAM terbatas, pastikan kontainer
> `bigdata-spark` dari Modul 9 **dihentikan sementara** saat menjalankan setup awal:
> ```bash
> bash stop.sh   # dari direktori bigdata-spark/
> ```
> Kontainer `bigdata-spark` baru perlu dinyalakan kembali saat Langkah 8 (ekspor data).

---

## 2. Struktur Folder

```
praktikum-bigdata/
└── modul10/
    ├── docker-compose-modul10.yml     ← definisi 5 layanan
    ├── prometheus/
    │   └── prometheus.yml             ← konfigurasi scrape targets
    ├── grafana/
    │   └── provisioning/
    │       └── datasources/
    │           └── prometheus.yml     ← auto-provisioning datasource
    ├── scripts/
    │   ├── buat_data_ecommerce.py     ← generator dataset e-commerce ke HDFS
    │   ├── persiapan_data_analitik.py ← Spark: HDFS → agregasi Gold layer
    │   └── ekspor_ke_postgresql.py    ← Spark: Gold layer → PostgreSQL
    ├── data/
    │   ├── transaksi_ecommerce.json   ← 12.000 baris dataset siap pakai
    │   ├── pelanggan.json             ← 300 data pelanggan dengan segmen & kota
    │   └── referensi_schema.json      ← dokumentasi schema semua tabel
    └── README.md
```

Buat struktur direktori:

```bash
mkdir -p praktikum-bigdata/modul10/{prometheus,grafana/provisioning/datasources,scripts,data}
cd praktikum-bigdata/modul10
```

---

## 3. Membuat Semua File Konfigurasi

Jalankan semua perintah berikut dari dalam direktori `modul10/`.

### 3.1 `docker-compose-modul10.yml`

```bash
cat > docker-compose-modul10.yml << 'EOF'
version: "3.8"

services:

  # ── PostgreSQL: Backend Superset & Penyimpanan Data Analitik ──
  postgres:
    image: postgres:15
    container_name: modul10-postgres
    environment:
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: superset
      POSTGRES_DB: analytics
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    networks:
      - modul10-net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U superset -d analytics"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ── Apache Superset ──────────────────────────────────────────
  superset:
    image: apache/superset:3.1.0
    container_name: modul10-superset
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      SUPERSET_SECRET_KEY: "modul10-secret-key-ganti-di-produksi"
      DATABASE_URL: "postgresql+psycopg2://superset:superset@postgres:5432/analytics"
    ports:
      - "8088:8088"
    networks:
      - modul10-net
    command: >
      bash -c "
        pip install psycopg2-binary --quiet &&
        superset db upgrade &&
        superset fab create-admin
          --username admin
          --firstname Admin
          --lastname User
          --email admin@example.com
          --password admin &&
        superset init &&
        superset run -h 0.0.0.0 -p 8088 --with-threads --reload
      "

  # ── Prometheus ───────────────────────────────────────────────
  prometheus:
    image: prom/prometheus:v2.51.2
    container_name: modul10-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    networks:
      - modul10-net
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--storage.tsdb.retention.time=7d'
      - '--web.enable-lifecycle'
      - '--web.console.libraries=/usr/share/prometheus/console_libraries'
      - '--web.console.templates=/usr/share/prometheus/consoles'

  # ── Node Exporter (metrik OS host) ──────────────────────────
  node-exporter:
    image: prom/node-exporter:v1.7.0
    container_name: modul10-node-exporter
    ports:
      - "9100:9100"
    networks:
      - modul10-net
    pid: host
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro
    command:
      - '--path.procfs=/host/proc'
      - '--path.sysfs=/host/sys'
      - '--collector.filesystem.ignored-mount-points=^/(sys|proc|dev|host|etc)($$|/)'
      - '--collector.disable-defaults'
      - '--collector.cpu'
      - '--collector.meminfo'
      - '--collector.loadavg'
      - '--collector.filesystem'
      - '--collector.netdev'
      - '--collector.diskstats'

  # ── Grafana ──────────────────────────────────────────────────
  grafana:
    image: grafana/grafana:10.2.0
    container_name: modul10-grafana
    depends_on:
      - prometheus
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_USERS_ALLOW_SIGN_UP: "false"
      GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH: ""
      GF_ANALYTICS_REPORTING_ENABLED: "false"
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
    networks:
      - modul10-net

volumes:
  pgdata:
  prometheus-data:
  grafana-data:

networks:
  modul10-net:
    driver: bridge
EOF
```

---

### 3.2 `prometheus/prometheus.yml`

```bash
cat > prometheus/prometheus.yml << 'EOF'
global:
  scrape_interval:     15s
  evaluation_interval: 15s
  scrape_timeout:      10s

# Tidak ada rule files untuk lab ini
rule_files: []

scrape_configs:

  # Prometheus memantau dirinya sendiri
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  # Node Exporter: metrik OS host (CPU, memori, disk, network)
  - job_name: 'node-exporter'
    static_configs:
      - targets: ['node-exporter:9100']
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        regex: '([^:]+).*'
        replacement: 'bigdata-host'
EOF
```

---

### 3.3 `grafana/provisioning/datasources/prometheus.yml`

```bash
cat > grafana/provisioning/datasources/prometheus.yml << 'EOF'
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
    jsonData:
      timeInterval: "15s"
      httpMethod: POST
EOF
```

---

### 3.4 `grafana/provisioning/dashboards/dashboard.yml`

```bash
mkdir -p grafana/provisioning/dashboards

cat > grafana/provisioning/dashboards/dashboard.yml << 'EOF'
apiVersion: 1

providers:
  - name: 'default'
    orgId: 1
    folder: ''
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /etc/grafana/provisioning/dashboards
EOF
```

---

### 3.5 `README.md`

```bash
cat > README.md << 'EOF'
# Modul 10 — Monitoring, Visualisasi, dan Eksplorasi Big Data

## Langkah Cepat

```bash
# 1. Jalankan semua layanan
docker compose -f docker-compose-modul10.yml up -d

# 2. Tunggu ~3 menit, lalu cek status
docker compose -f docker-compose-modul10.yml ps

# 3. Siapkan data (perlu kontainer bigdata-spark dari Modul 9)
bash start.sh && bash login.sh   # dari direktori bigdata-spark/
spark-submit --master yarn --deploy-mode client \
  --executor-memory 512m --num-executors 2 \
  /modul10/scripts/buat_data_ecommerce.py

# 4. Agregasi dan ekspor ke PostgreSQL
spark-submit --master yarn --deploy-mode client \
  --executor-memory 512m --num-executors 2 \
  --jars /opt/spark/jars/postgresql-42.7.3.jar \
  /modul10/scripts/persiapan_data_analitik.py

spark-submit --master yarn --deploy-mode client \
  --executor-memory 512m --num-executors 2 \
  --jars /opt/spark/jars/postgresql-42.7.3.jar \
  /modul10/scripts/ekspor_ke_postgresql.py
```

## UI

| Layanan | URL | Login |
|---|---|---|
| Apache Superset | http://localhost:8088 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Node Exporter | http://localhost:9100/metrics | — |
| PostgreSQL | localhost:5432 | superset / superset |

## Menghentikan Layanan

```bash
docker compose -f docker-compose-modul10.yml down        # simpan data
docker compose -f docker-compose-modul10.yml down -v     # hapus volume
```
EOF
```

---

## 4. Menjalankan Semua Layanan

### Langkah 4.1 — Verifikasi file konfigurasi tersedia

```bash
ls -la docker-compose-modul10.yml \
       prometheus/prometheus.yml \
       grafana/provisioning/datasources/prometheus.yml
```

Semua tiga file harus ada sebelum melanjutkan.

### Langkah 4.2 — Jalankan semua container

```bash
docker compose -f docker-compose-modul10.yml up -d
```

### Langkah 4.3 — Pantau proses startup

```bash
# Cek status container (jalankan beberapa kali selama startup)
docker compose -f docker-compose-modul10.yml ps
```

Output target setelah ~3 menit:

```
NAME                    IMAGE                      STATUS
modul10-postgres        postgres:15                Up (healthy)
modul10-superset        apache/superset:3.1.0      Up
modul10-prometheus      prom/prometheus:v2.51.2    Up
modul10-node-exporter   prom/node-exporter:v1.7.0  Up
modul10-grafana         grafana/grafana:10.2.0     Up
```

### Langkah 4.4 — Pantau log Superset (paling lambat startup)

```bash
docker compose -f docker-compose-modul10.yml logs -f superset
```

Tunggu hingga muncul baris berikut lalu tekan `Ctrl+C`:

```
[INFO] Booting worker with pid: ...
```

> Superset membutuhkan **2–5 menit** pada startup pertama karena menjalankan
> `superset db upgrade` dan `superset init` untuk menginisialisasi database metadata.

---

## 5. Verifikasi Setiap Layanan

### Langkah 5.1 — Verifikasi PostgreSQL

```bash
docker exec -it modul10-postgres \
  psql -U superset -d analytics -c "SELECT version();"
```

Output yang diharapkan:

```
PostgreSQL 15.x on x86_64-pc-linux-gnu ...
```

### Langkah 5.2 — Verifikasi Prometheus scraping Node Exporter

```bash
# Cek targets yang dipantau Prometheus
curl -s http://localhost:9090/api/v1/targets \
  | python3 -m json.tool \
  | grep -E '"health"|"job"'
```

Output yang diharapkan:

```json
"health": "up",
"job": "node-exporter",
"health": "up",
"job": "prometheus",
```

Atau buka langsung di browser: `http://localhost:9090/targets`

Pastikan **State = UP** untuk kedua target.

### Langkah 5.3 — Verifikasi Node Exporter mengirim metrik

```bash
# Cek beberapa metrik CPU dari Node Exporter
curl -s http://localhost:9100/metrics | grep "^node_cpu_seconds_total" | head -5
```

Output yang diharapkan (beberapa baris metrik):

```
node_cpu_seconds_total{cpu="0",mode="idle"} 12345.67
node_cpu_seconds_total{cpu="0",mode="user"} 234.56
...
```

### Langkah 5.4 — Verifikasi Grafana bisa query Prometheus

```bash
curl -s -u admin:admin \
  "http://localhost:3000/api/datasources" \
  | python3 -m json.tool | grep -E '"name"|"type"|"url"'
```

Output yang diharapkan:

```json
"name": "Prometheus",
"type": "prometheus",
"url": "http://prometheus:9090",
```

---

## 6. Mengakses Semua UI

Buka browser di komputer host. Semua URL di bawah diakses dari host, **bukan dari dalam container**.

### Apache Superset — `http://localhost:8088`

Login: **admin / admin**

| Menu | Fungsi |
|---|---|
| **Dashboards** | Lihat dan buat dashboard interaktif |
| **Charts** | Kelola semua visualisasi individual |
| **Data → Datasets** | Daftarkan tabel PostgreSQL sebagai dataset |
| **Settings → Database Connections** | Tambah koneksi ke PostgreSQL |
| **SQL Lab** | Query SQL ad-hoc langsung ke PostgreSQL |

> Jika halaman login belum muncul, tunggu 1–2 menit lagi lalu refresh.
> Superset masih dalam proses inisialisasi jika log masih menampilkan
> `Running upgrade` atau `Creating default roles`.

### Prometheus — `http://localhost:9090`

Tidak perlu login.

| Halaman | Fungsi |
|---|---|
| `/graph` | Expression Browser — jalankan query PromQL |
| `/targets` | Cek status semua scrape target |
| `/metrics` | Metrik Prometheus itu sendiri |
| `/config` | Lihat konfigurasi yang aktif |

**Test query pertama di Expression Browser:**

```promql
node_memory_MemTotal_bytes / 1024 / 1024 / 1024
```

Hasil menampilkan total RAM host dalam GB.

### Grafana — `http://localhost:3000`

Login: **admin / admin** (akan diminta ganti password, klik **Skip** untuk lab)

| Menu | Fungsi |
|---|---|
| **Dashboards** | Buat dan kelola dashboard panel |
| **Connections → Data Sources** | Verifikasi datasource Prometheus |
| **Explore** | Query PromQL ad-hoc dengan visualisasi langsung |
| **Alerting** | Konfigurasi alert berdasarkan threshold metrik |

**Verifikasi datasource Prometheus di Grafana:**
1. Klik **Connections → Data Sources**
2. Klik **Prometheus**
3. Scroll ke bawah, klik **Save & Test**
4. Pastikan muncul: *"Successfully queried the Prometheus API"*

### Node Exporter — `http://localhost:9100/metrics`

Tidak perlu login. Halaman ini menampilkan **semua metrik** dalam format teks Prometheus.
Berguna untuk verifikasi metrik tersedia sebelum membuat panel di Grafana.

**Contoh metrik penting yang bisa dicari (Ctrl+F):**

```
node_cpu_seconds_total
node_memory_MemAvailable_bytes
node_load1
node_filesystem_avail_bytes
node_network_receive_bytes_total
```

---

## 7. Menghubungkan Grafana ke Prometheus

Datasource Prometheus sudah di-*provision* otomatis melalui file
`grafana/provisioning/datasources/prometheus.yml`. Lakukan verifikasi manual:

### Langkah 7.1 — Buka halaman Data Sources

Navigasi: **Connections → Data Sources → Prometheus**

### Langkah 7.2 — Klik "Save & Test"

Pastikan muncul banner hijau:
> *"Successfully queried the Prometheus API."*

### Langkah 7.3 — Test query PromQL di Grafana Explore

1. Klik menu **Explore** (ikon kompas di sidebar kiri)
2. Pastikan datasource terpilih adalah **Prometheus**
3. Ketik query berikut di kolom Metrics:

```promql
100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)
```

4. Klik **Run Query** (atau tekan `Shift+Enter`)
5. Grafik CPU usage seharusnya muncul

Jika grafik muncul, Grafana sudah terhubung ke Prometheus dengan benar.

---

## 8. Persiapan Data dan Inisialisasi untuk Latihan

### Data yang Digunakan Modul 10

Modul 10 menggunakan **dua sumber data** dengan jalur yang berbeda:

| Sumber | Format | Untuk Apa |
|---|---|---|
| Dataset e-commerce (transaksi + pelanggan) | Parquet di HDFS → CSV di Gold → PostgreSQL | Apache Superset (visualisasi bisnis) |
| Metrik sistem host | Dikumpulkan real-time oleh Node Exporter | Prometheus → Grafana (monitoring) |

Data monitoring untuk Grafana **tidak perlu disiapkan** — Node Exporter secara otomatis mengumpulkan metrik CPU, memori, disk, dan network dari host setiap 15 detik.

Yang perlu disiapkan adalah **data bisnis e-commerce** berikut ini.

---

### Tabel Analitik yang Dibutuhkan di PostgreSQL

| Tabel PostgreSQL | Dibuat dari | Kolom Utama | Digunakan di Chart |
|---|---|---|---|
| `tren_bulanan` | Agregasi transaksi per bulan | tahun, bulan, periode, omzet, jumlah_transaksi, rata_transaksi, ma3_omzet, mom_growth_pct | Line Chart, Big Number |
| `omzet_kategori` | Agregasi transaksi per kategori | kategori, omzet_total, jumlah_transaksi, omzet_rata, persen_omzet | Bar Chart |
| `omzet_kota` | Agregasi transaksi per kota | kota, omzet, transaksi, pelanggan_unik | Table, Bar Chart |

---

### Inisialisasi Data — Step by Step

#### Step 1 — Pastikan semua container Modul 10 berjalan

```bash
docker compose -f docker-compose-modul10.yml ps
# Semua STATUS harus Up
```

#### Step 2 — Nyalakan kontainer bigdata-spark dari Modul 9

```bash
# Dari direktori bigdata-spark/
bash start.sh
bash login.sh
```

Verifikasi layanan Hadoop aktif:

```bash
jps
# Harus ada: NameNode, DataNode, ResourceManager, NodeManager
```

#### Step 3 — Mount direktori modul10 ke dalam kontainer bigdata-spark

Agar skrip Python di folder `modul10/scripts/` dapat diakses dari dalam
kontainer, tambahkan volume mount di `docker-compose.yml` milik `bigdata-spark`
atau salin file secara manual:

```bash
# Dari luar kontainer (WSL), salin skrip ke kontainer
docker cp modul10/scripts/buat_data_ecommerce.py \
          bigdata-spark:/tmp/buat_data_ecommerce.py

docker cp modul10/scripts/persiapan_data_analitik.py \
          bigdata-spark:/tmp/persiapan_data_analitik.py

docker cp modul10/scripts/ekspor_ke_postgresql.py \
          bigdata-spark:/tmp/ekspor_ke_postgresql.py
```

#### Step 4 — Generate dataset e-commerce ke HDFS

Di dalam kontainer `bigdata-spark`:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  /tmp/buat_data_ecommerce.py
```

Output yang diharapkan:

```
[OK] Transaksi : 12000 baris → hdfs:///datalake/silver/transaksi_ecommerce/
[OK] Pelanggan :   300 baris → hdfs:///datalake/bronze/pelanggan/
```

#### Step 5 — Jalankan agregasi Spark → Gold layer

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  /tmp/persiapan_data_analitik.py
```

Output yang diharapkan:

```
[1/3] Tren bulanan   → hdfs:///datalake/gold/tren_lanjutan/
[2/3] Omzet kategori → hdfs:///datalake/gold/omzet_kategori/
[3/3] Omzet kota     → hdfs:///datalake/gold/omzet_kota/
[OK] Semua tabel analitik siap.
```

Verifikasi Gold layer:

```bash
hdfs dfs -ls /datalake/gold/
hdfs dfs -du -h /datalake/gold/tren_lanjutan/
hdfs dfs -du -h /datalake/gold/omzet_kategori/
hdfs dfs -du -h /datalake/gold/omzet_kota/
```

#### Step 6 — Unduh JDBC driver PostgreSQL

```bash
# Di dalam kontainer bigdata-spark
wget -q -P /opt/spark/jars/ \
  https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Verifikasi
ls -lh /opt/spark/jars/postgresql-42.7.3.jar
```

#### Step 7 — Ekspor data ke PostgreSQL Modul 10

> **Catatan jaringan:** Container `bigdata-spark` dan `modul10-postgres` berada
> di jaringan Docker yang berbeda. Gunakan alamat IP host Docker sebagai host
> PostgreSQL, bukan `localhost`.

Cari IP host Docker dari dalam kontainer:

```bash
# Di dalam kontainer bigdata-spark
ip route | grep default | awk '{print $3}'
# Biasanya: 172.17.0.1 atau 192.168.65.1 (Docker Desktop di Mac)
```

Jalankan ekspor dengan IP host yang ditemukan:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  --jars /opt/spark/jars/postgresql-42.7.3.jar \
  /tmp/ekspor_ke_postgresql.py
```

Output yang diharapkan:

```
[1/3] Ekspor tren_bulanan    → PostgreSQL ... OK (24 baris)
[2/3] Ekspor omzet_kategori  → PostgreSQL ... OK (6 baris)
[3/3] Ekspor omzet_kota      → PostgreSQL ... OK (20 baris)
[OK] Semua data berhasil diekspor ke PostgreSQL.
```

#### Step 8 — Verifikasi tabel di PostgreSQL

```bash
# Dari luar kontainer (WSL)
docker exec -it modul10-postgres \
  psql -U superset -d analytics -c "\dt"
```

Output yang diharapkan:

```
         List of relations
 Schema |      Name       | Type  |  Owner
--------+-----------------+-------+---------
 public | ab_permission   | table | superset
 public | ab_role         | table | superset
 ...
 public | omzet_kategori  | table | superset
 public | omzet_kota      | table | superset
 public | tren_bulanan    | table | superset
```

Cek jumlah baris setiap tabel:

```bash
docker exec -it modul10-postgres psql -U superset -d analytics -c "
  SELECT 'tren_bulanan'   AS tabel, COUNT(*) FROM tren_bulanan
  UNION ALL
  SELECT 'omzet_kategori', COUNT(*) FROM omzet_kategori
  UNION ALL
  SELECT 'omzet_kota',     COUNT(*) FROM omzet_kota;
"
```

Output yang diharapkan:

```
    tabel       | count
----------------+-------
 tren_bulanan   |    24
 omzet_kategori |     6
 omzet_kota     |    20
```

#### Step 9 — Data siap untuk latihan

Setelah Step 8 berhasil, mahasiswa bisa langsung memulai Tahap 2 latihan
(mendaftarkan koneksi database di Superset dan membuat visualisasi).

---

### Reset Data (mulai ulang dari nol)

```bash
# Hapus semua tabel di PostgreSQL
docker exec -it modul10-postgres psql -U superset -d analytics -c "
  DROP TABLE IF EXISTS tren_bulanan, omzet_kategori, omzet_kota;
"

# Hapus data HDFS e-commerce
docker exec bigdata-spark hdfs dfs -rm -r \
  /datalake/silver/transaksi_ecommerce \
  /datalake/bronze/pelanggan \
  /datalake/gold/tren_lanjutan \
  /datalake/gold/omzet_kategori \
  /datalake/gold/omzet_kota

# Ulangi dari Step 4
```

---

## 9. Checklist Sebelum Memulai Latihan

**Container Modul 10:**
- [ ] `docker compose -f docker-compose-modul10.yml ps` — semua **5 container** berstatus `Up`
- [ ] `modul10-superset` tidak memiliki error di log (`logs superset | tail -20`)
- [ ] `modul10-postgres` berstatus `Up (healthy)`

**Akses UI:**
- [ ] Superset terbuka di `http://localhost:8088` — bisa login `admin/admin`
- [ ] Prometheus terbuka di `http://localhost:9090/targets` — node-exporter **State=UP**
- [ ] Grafana terbuka di `http://localhost:3000` — bisa login `admin/admin`
- [ ] Node Exporter `http://localhost:9100/metrics` — menampilkan metrik teks

**Grafana + Prometheus:**
- [ ] **Connections → Data Sources → Prometheus → Save & Test** menampilkan *"Successfully queried"*
- [ ] **Explore** dengan query `node_load1` menghasilkan nilai numerik

**Data bisnis (PostgreSQL):**
- [ ] `docker exec modul10-postgres psql ... -c "\dt"` menampilkan `tren_bulanan`, `omzet_kategori`, `omzet_kota`
- [ ] Jumlah baris: `tren_bulanan` = 24, `omzet_kategori` = 6, `omzet_kota` = 20

**Superset ↔ PostgreSQL:**
- [ ] Database Connection "Analitik E-Commerce" terdaftar di **Settings → Database Connections**
- [ ] **Test Connection** sukses

Jika semua centang terpenuhi, lingkungan lab **siap digunakan** untuk seluruh latihan Modul 10.

---

## Ringkasan Port dan Layanan

| Port | Layanan | URL | Kredensial |
|---|---|---|---|
| 8088 | Apache Superset | `http://localhost:8088` | admin / admin |
| 9090 | Prometheus | `http://localhost:9090` | — |
| 3000 | Grafana | `http://localhost:3000` | admin / admin |
| 9100 | Node Exporter | `http://localhost:9100/metrics` | — |
| 5432 | PostgreSQL | `localhost:5432` | superset / superset |

