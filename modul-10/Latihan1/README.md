# Latihan 1 — Persiapan Lingkungan dan Data
**Modul 10 · Monitoring, Visualisasi, dan Eksplorasi Big Data** | Estimasi waktu: **15 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Menjalankan dan memverifikasi semua layanan Docker Modul 10 (Superset, PostgreSQL, Prometheus, Node Exporter, Grafana)
- Menyiapkan data analitik e-commerce menggunakan Apache Spark dan menyimpannya ke Gold layer HDFS
- Mengekspor hasil agregasi Spark ke PostgreSQL sebagai sumber data Superset
- Memverifikasi kesiapan setiap layanan melalui UI dan CLI sebelum memulai latihan selanjutnya

---

## Prasyarat

- [ ] Docker Engine berjalan
- [ ] Direktori `praktikum-bigdata/modul10/` tersedia dengan seluruh file dari panduan setup
- [ ] Kontainer `bigdata-spark` dari Modul 9 dapat dijalankan (`docker ps -a | grep bigdata-spark`)
- [ ] File `buat_data_ecommerce.py`, `persiapan_data_analitik.py`, dan `ekspor_ke_postgresql.py` tersedia di `modul10/scripts/`

---

## Langkah Kerja

### Langkah 1.1 — Jalankan semua layanan Docker Modul 10

Buka terminal, arahkan ke direktori `modul10/`, lalu verifikasi file konfigurasi tersedia:

```bash
ls docker-compose-modul10.yml prometheus/ grafana/
```

Semua tiga path harus ada. Kemudian jalankan semua layanan:

```bash
docker compose -f docker-compose-modul10.yml up -d
```

Tunggu sekitar **3 menit**, lalu cek status:

```bash
docker compose -f docker-compose-modul10.yml ps
```

**Output yang diharapkan (semua STATUS = Up):**

```
NAME                    IMAGE                      STATUS
modul10-postgres        postgres:15                Up (healthy)
modul10-superset        apache/superset:3.1.0      Up
modul10-prometheus      prom/prometheus:v2.51.2    Up
modul10-node-exporter   prom/node-exporter:v1.7.0  Up
modul10-grafana         grafana/grafana:10.2.0     Up
```

> Jika `modul10-superset` belum `Up`, pantau log-nya dengan:
> ```bash
> docker compose -f docker-compose-modul10.yml logs -f superset
> ```
> Tunggu hingga muncul `Booting worker with pid`. Proses ini bisa memakan waktu
> hingga **5 menit** pada startup pertama.

---

### Langkah 1.2 — Verifikasi akses ke setiap layanan via browser

Buka browser dan akses satu per satu URL berikut:

| Layanan | URL | Login | Yang Diverifikasi |
|---|---|---|---|
| Apache Superset | `http://localhost:8088` | admin / admin | Halaman login berhasil |
| Prometheus | `http://localhost:9090` | — | Status → Targets |
| Grafana | `http://localhost:3000` | admin / admin | Halaman login berhasil |
| Node Exporter | `http://localhost:9100/metrics` | — | Teks metrik tampil |

**Verifikasi kritis di Prometheus:**

Navigasi ke `http://localhost:9090/targets`. Pastikan kedua target ini berstatus **UP**:

```
node-exporter   http://node-exporter:9100/metrics   UP
prometheus      http://localhost:9090/metrics        UP
```

Jika `node-exporter` berstatus `DOWN`, tunggu 30 detik lagi dan refresh halaman.

---

### Langkah 1.3 — Nyalakan kontainer bigdata-spark dan siapkan data

Buka **terminal baru** (terpisah dari terminal Docker Modul 10). Masuk ke direktori `bigdata-spark/` dan nyalakan kontainer:

```bash
bash start.sh
bash login.sh
```

Verifikasi layanan Hadoop aktif di dalam kontainer:

```bash
jps
```

Pastikan semua empat proses muncul: `NameNode`, `DataNode`, `ResourceManager`, `NodeManager`. Jika belum:

```bash
start-dfs.sh && start-yarn.sh
sleep 15 && jps
```

---

### Langkah 1.4 — Salin skrip ke dalam kontainer bigdata-spark

Buka terminal ketiga (dari luar kontainer, di WSL), salin ketiga skrip:

```bash
docker cp modul10/scripts/buat_data_ecommerce.py \
           bigdata-spark:/tmp/buat_data_ecommerce.py

docker cp modul10/scripts/persiapan_data_analitik.py \
           bigdata-spark:/tmp/persiapan_data_analitik.py

docker cp modul10/scripts/ekspor_ke_postgresql.py \
           bigdata-spark:/tmp/ekspor_ke_postgresql.py
```

Verifikasi file tersalin (di dalam kontainer):

```bash
ls -lh /tmp/buat_data_ecommerce.py \
        /tmp/persiapan_data_analitik.py \
        /tmp/ekspor_ke_postgresql.py
```

---

### Langkah 1.5 — Generate dataset e-commerce ke HDFS

Di dalam kontainer `bigdata-spark`, jalankan:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  /tmp/buat_data_ecommerce.py
```

Proses ini membutuhkan **2–4 menit**. Output akhir yang diharapkan:

```
[OK] Transaksi : 12000 baris → hdfs:///datalake/silver/transaksi_ecommerce/
[OK] Pelanggan :   300 baris → hdfs:///datalake/bronze/pelanggan/
```

Verifikasi data tersimpan di HDFS:

```bash
hdfs dfs -ls /datalake/silver/transaksi_ecommerce/
hdfs dfs -du -h /datalake/silver/transaksi_ecommerce/
hdfs dfs -ls /datalake/bronze/pelanggan/
```

Catat ukuran data pada **Tabel 1.2**.

---

### Langkah 1.6 — Jalankan agregasi Spark dan simpan ke Gold layer

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  /tmp/persiapan_data_analitik.py
```

Proses ini membutuhkan **3–5 menit**. Output yang diharapkan:

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

---

### Langkah 1.7 — Unduh driver JDBC dan ekspor ke PostgreSQL

Pastikan driver JDBC PostgreSQL tersedia di dalam kontainer:

```bash
ls -lh /opt/spark/jars/postgresql-42.7.3.jar
```

Jika belum ada, unduh:

```bash
wget -q -P /opt/spark/jars/ \
  https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
echo "[OK] Driver JDBC tersedia: $(ls -lh /opt/spark/jars/postgresql-42.7.3.jar)"
```

Temukan IP host Docker (dibutuhkan agar Spark bisa menjangkau PostgreSQL di jaringan berbeda):

```bash
DOCKER_HOST_IP=$(ip route | grep default | awk '{print $3}')
echo "IP host Docker: $DOCKER_HOST_IP"
```

Catat nilai IP ini pada **Tabel 1.1** — akan dibutuhkan lagi jika ada troubleshooting.

Jalankan ekspor ke PostgreSQL:

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
[1/3] Ekspor tren_bulanan   → PostgreSQL ... OK (24 baris)
[2/3] Ekspor omzet_kategori → PostgreSQL ... OK (6 baris)
[3/3] Ekspor omzet_kota     → PostgreSQL ... OK (20 baris)
[OK] Semua data berhasil diekspor ke PostgreSQL.
```

---

### Langkah 1.8 — Verifikasi tabel di PostgreSQL

Dari terminal di luar kontainer (WSL), jalankan:

```bash
docker exec -it modul10-postgres \
  psql -U superset -d analytics -c "\dt"
```

Kemudian verifikasi jumlah baris setiap tabel:

```bash
docker exec -it modul10-postgres psql -U superset -d analytics -c "
  SELECT 'tren_bulanan'   AS tabel, COUNT(*) AS baris FROM tren_bulanan
  UNION ALL
  SELECT 'omzet_kategori', COUNT(*) FROM omzet_kategori
  UNION ALL
  SELECT 'omzet_kota',     COUNT(*) FROM omzet_kota;
"
```

Cek beberapa baris data dari setiap tabel:

```bash
# Cek isi tren_bulanan
docker exec -it modul10-postgres psql -U superset -d analytics \
  -c "SELECT periode, omzet, ma3_omzet, mom_growth_pct FROM tren_bulanan ORDER BY periode LIMIT 5;"

# Cek isi omzet_kategori
docker exec -it modul10-postgres psql -U superset -d analytics \
  -c "SELECT kategori, omzet_total, persen_omzet FROM omzet_kategori ORDER BY omzet_total DESC;"

# Cek isi omzet_kota
docker exec -it modul10-postgres psql -U superset -d analytics \
  -c "SELECT kota, omzet, transaksi FROM omzet_kota ORDER BY omzet DESC LIMIT 5;"
```

Catat semua nilai pada **Tabel 1.3**.

---

## Tabel Pencatatan Hasil

### Tabel 1.1 — Status Semua Layanan

| Komponen | Port | Status (Up/Down) | Waktu Startup | Catatan |
|---|---|---|---|---|
| modul10-postgres | 5432 | _..._ | _..._ | healthy? |
| modul10-superset | 8088 | _..._ | _..._ | login berhasil? |
| modul10-prometheus | 9090 | _..._ | _..._ | targets UP? |
| modul10-node-exporter | 9100 | _..._ | _..._ | metrik tampil? |
| modul10-grafana | 3000 | _..._ | _..._ | login berhasil? |
| **IP host Docker** | — | _..._ | — | untuk koneksi JDBC |

**Status Prometheus Targets:**

| Target | URL | State | Last Scrape |
|---|---|---|---|
| node-exporter | `http://node-exporter:9100/metrics` | _UP/DOWN_ | _..._ |
| prometheus | `http://localhost:9090/metrics` | _UP/DOWN_ | _..._ |

### Tabel 1.2 — Data di HDFS Setelah Generate

| Layer | Path HDFS | Ukuran | Jumlah File |
|---|---|---|---|
| Silver | `/datalake/silver/transaksi_ecommerce/` | _..._ KB | _..._ |
| Bronze | `/datalake/bronze/pelanggan/` | _..._ KB | _..._ |
| Gold | `/datalake/gold/tren_lanjutan/` | _..._ KB | _..._ |
| Gold | `/datalake/gold/omzet_kategori/` | _..._ KB | _..._ |
| Gold | `/datalake/gold/omzet_kota/` | _..._ KB | _..._ |

### Tabel 1.3 — Data di PostgreSQL Setelah Ekspor

| Tabel | Jumlah Baris | Kolom Utama | Contoh Nilai Pertama |
|---|---|---|---|
| `tren_bulanan` | _..._ | periode, omzet, ma3_omzet | _..._ |
| `omzet_kategori` | _..._ | kategori, omzet_total | _..._ |
| `omzet_kota` | _..._ | kota, omzet, transaksi | _..._ |

**Isi 3 baris pertama `tren_bulanan` (salin dari output psql):**

```
(tempel output di sini)
```

**Kategori yang muncul di `omzet_kategori` (salin dari output psql):**

```
(tempel output di sini)
```

---

## Refleksi dan Analisis

**R1.1 — Modul 10 menjalankan 5 container sekaligus di Docker, sementara Modul 8 hanya 2 dan Modul 9 hanya 1. Dari sisi arsitektur, mengapa setiap layanan (Superset, PostgreSQL, Prometheus, Grafana) dijalankan sebagai container terpisah alih-alih satu container besar? Apa keuntungan dan kerugian pendekatan ini?**

> Petunjuk: Pikirkan tentang prinsip *separation of concerns* di microservices. Jika Superset crash, apakah PostgreSQL ikut crash?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.2 — Pada Langkah 1.7, koneksi JDBC dari Spark (di dalam `bigdata-spark`) ke PostgreSQL (di dalam `modul10-postgres`) tidak bisa menggunakan `localhost` atau nama container. Jelaskan mengapa demikian, dan apa yang membuat IP host Docker (`172.17.0.1` atau sejenisnya) menjadi solusi yang tepat.**

> Petunjuk: Pikirkan tentang jaringan Docker — setiap `docker compose` membuat jaringan virtual tersendiri (`modul10-net`). Container di jaringan berbeda tidak bisa saling menjangkau secara langsung via nama service.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.3 — Dari Tabel 1.3, tabel `tren_bulanan` memiliki 24 baris. Mengapa hasilnya tepat 24 baris? Dari mana angka ini berasal? Apa yang akan terjadi pada jumlah baris ini jika dataset transaksi mencakup 3 tahun penuh?**

> Petunjuk: Tabel ini adalah hasil `groupBy("tahun", "bulan")` pada transaksi 2 tahun terakhir (12 bulan × 2 tahun = 24 kombinasi unik periode).
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.4 — Superset tidak menyimpan data sendiri — ia hanya menjalankan query ke database yang terhubung (PostgreSQL). Apa keuntungan arsitektur "thin BI layer" seperti ini dibandingkan sistem BI yang menyimpan data sendiri (data copy)? Apa risikonya jika PostgreSQL mengalami downtime?**

>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.5 — Dari Tabel 1.2, bandingkan ukuran data di Silver (12.000 baris Parquet) dengan ukuran di Gold (tabel agregat). Mengapa ukuran Gold jauh lebih kecil? Dalam arsitektur data lake produksi, mengapa Gold layer tetap diperlukan meskipun Superset bisa langsung query ke Silver?**

>
> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 1

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Sebanyak **___** layanan berhasil dijalankan melalui Docker Compose. Data e-commerce dengan **___** baris transaksi berhasil diproses Spark menjadi **___** tabel analitik di Gold layer HDFS, kemudian diekspor ke PostgreSQL. Tabel `tren_bulanan` memiliki **___** baris karena dataset mencakup **___** bulan. Koneksi JDBC dari Spark ke PostgreSQL membutuhkan IP host **___** karena kedua container berada di **___** jaringan Docker yang berbeda."

---

*Latihan 1 selesai. Lanjutkan ke **Latihan 2 — Eksplorasi Data dan Visualisasi Superset**.*