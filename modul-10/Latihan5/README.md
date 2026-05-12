# Latihan 5 — Eksplorasi Lanjutan: PromQL, SQL Lab, dan Diskusi
**Modul 10 · Monitoring, Visualisasi, dan Eksplorasi Big Data** | Estimasi waktu: **15 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Menulis query PromQL lanjutan untuk metrik disk, network, dan uptime sistem
- Membuat chart tambahan di Superset menggunakan SQL query kustom via SQL Lab
- Menjawab pertanyaan diskusi yang menghubungkan Superset dan Grafana dalam konteks arsitektur observabilitas Big Data
- Merangkum seluruh pengalaman praktikum Modul 10 dalam satu narasi terpadu

---

## Prasyarat

- [ ] Latihan 1–4 sudah selesai
- [ ] Prometheus Expression Browser dapat diakses di `http://localhost:9090/graph`
- [ ] Superset SQL Lab dapat diakses di `http://localhost:8088`
- [ ] Dataset `omzet_kota`, `omzet_kategori`, `tren_bulanan` tersedia di Superset

---

## Bagian A — Eksplorasi Query PromQL Lanjutan

### Langkah A.1 — Buka Prometheus Expression Browser

Navigasi ke `http://localhost:9090/graph`.

Jalankan setiap query di bawah satu per satu. Untuk setiap query:
1. Ketik query di kolom input
2. Klik **Execute**
3. Klik tab **Graph** untuk melihat visualisasi time-series
4. Klik tab **Table** untuk melihat nilai numerik terbaru
5. Catat nilai dan pengamatan pada **Tabel A.1**

---

### Langkah A.2 — Query 1: Daftar semua metrik Node Exporter

```promql
{job="node-exporter"}
```

Jalankan query ini di tab **Table**. Hasilnya menampilkan seluruh metrik yang tersedia dari Node Exporter.

**Yang diamati:**
- Berapa total metrik yang dikumpulkan Node Exporter?
- Metrik apa yang berkaitan dengan filesystem, network, dan memory?
- Temukan metrik yang namanya mengandung kata `disk` atau `filesystem`

Catat temuan pada **Tabel A.1**.

---

### Langkah A.3 — Query 2: Penggunaan disk per filesystem

```promql
(node_filesystem_size_bytes{fstype!="tmpfs"} -
 node_filesystem_avail_bytes{fstype!="tmpfs"}) /
 node_filesystem_size_bytes{fstype!="tmpfs"} * 100
```

**Yang diamati:**
- Filesystem mana yang memiliki penggunaan disk tertinggi?
- Berapa persentase disk yang sudah terpakai?
- Apakah ada filesystem yang mendekati 80% kapasitas?

Catat pada **Tabel A.1**.

---

### Langkah A.4 — Query 3: Network receive throughput

```promql
rate(node_network_receive_bytes_total{device!~"lo|docker.*|br.*"}[5m]) / 1024
```

*(Filter `lo` = loopback, `docker.*` = interface Docker internal)*

**Yang diamati:**
- Interface network mana yang aktif menerima data?
- Berapa KB/s throughput saat ini?
- Apakah ada lonjakan throughput yang terdeteksi?

Kemudian jalankan query untuk network transmit:

```promql
rate(node_network_transmit_bytes_total{device!~"lo|docker.*|br.*"}[5m]) / 1024
```

Catat pada **Tabel A.1**.

---

### Langkah A.5 — Query 4: Uptime sistem dalam jam

```promql
(node_time_seconds - node_boot_time_seconds) / 3600
```

**Yang diamati:**
- Berapa jam sistem sudah berjalan?
- Jalankan ulang `docker compose -f docker-compose-modul10.yml restart node-exporter` lalu query kembali — apakah uptime berubah?

> Uptime yang diukur adalah uptime **host** (laptop/PC), bukan uptime container.
> Node Exporter mengakses `/proc` dan `/sys` dari host.

Catat pada **Tabel A.1**.

---

### Langkah A.6 — Query 5: Total memori yang digunakan container Docker

```promql
node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes
```

Kemudian konversi ke GB:

```promql
(node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes) / 1024 / 1024 / 1024
```

Bandingkan nilai ini dengan nilai di panel **Memory Usage (%)** di Grafana. Catat pada **Tabel A.1**.

---

### Langkah A.7 — Tambahkan panel Disk Usage ke dashboard Grafana (opsional)

Kembali ke dashboard `BigData Infrastructure Monitoring` di Grafana.

1. Klik **Edit** → **Add panel** → **Add visualization**
2. Pilih datasource: **Prometheus**
3. Masukkan query:

```promql
(node_filesystem_size_bytes{fstype!="tmpfs",mountpoint="/"} -
 node_filesystem_avail_bytes{fstype!="tmpfs",mountpoint="/"}) /
 node_filesystem_size_bytes{fstype!="tmpfs",mountpoint="/"} * 100
```

4. Konfigurasi:

| Setting | Nilai |
|---|---|
| Visualization | Gauge |
| Title | `Disk Usage Root (%)` |
| Unit | `Percent (0-100)` |
| Threshold | 0=Hijau, 70=Kuning, 90=Merah |

5. Klik **Apply** → **Save Dashboard**

---

## Bagian B — Chart Tambahan di Superset via SQL Lab

### Langkah B.1 — Buka SQL Lab

Navigasi ke **SQL Lab** di menu atas Superset (`http://localhost:8088`).

Pastikan database **Analitik E-Commerce** terpilih dan schema **public**.

---

### Langkah B.2 — Query analisis kota lanjutan

Jalankan query berikut:

```sql
SELECT
    kota,
    SUM(omzet)              AS total_omzet,
    SUM(transaksi)          AS total_transaksi,
    SUM(pelanggan_unik)     AS total_pelanggan,
    ROUND(
        SUM(omzet)::numeric /
        NULLIF(SUM(transaksi), 0), 0
    )                       AS avg_nilai_per_transaksi,
    ROUND(
        SUM(omzet)::numeric /
        NULLIF(SUM(pelanggan_unik), 0), 0
    )                       AS avg_nilai_per_pelanggan,
    ROUND(
        SUM(transaksi)::numeric /
        NULLIF(SUM(pelanggan_unik), 0), 2
    )                       AS frekuensi_belanja
FROM omzet_kota
GROUP BY kota
ORDER BY total_omzet DESC
LIMIT 15;
```

Klik **Run**. Catat hasil pada **Tabel B.1**.

Simpan query: klik **Save** → nama: `Analisis Kota Lanjutan` → klik **Save**.

---

### Langkah B.3 — Buat Horizontal Bar Chart dari hasil query

1. Setelah query berjalan, klik tombol **Explore** (di bawah hasil query) atau klik **Create Chart**
2. Pilih visualization type: **Bar Chart** (pastikan orientation = Horizontal, atau pilih **Horizontal Bar Chart** jika tersedia)
3. Konfigurasi:

   | Setting | Nilai |
   |---|---|
   | Metrics | `avg_nilai_per_transaksi` |
   | Dimensions | `kota` |
   | Sort By | `avg_nilai_per_transaksi` Descending |
   | Row Limit | `15` |
   | Show Legend | aktifkan |

4. Klik **Update Chart**
5. Simpan dengan nama: `Rata-rata Nilai Transaksi per Kota`

---

### Langkah B.4 — Query analisis tren kuartalan

Jalankan query berikut untuk melihat pola per kuartal:

```sql
SELECT
    EXTRACT(YEAR  FROM periode::date)    AS tahun,
    EXTRACT(QUARTER FROM periode::date)  AS kuartal,
    CONCAT(
        EXTRACT(YEAR FROM periode::date)::text,
        '-Q',
        EXTRACT(QUARTER FROM periode::date)::text
    )                                    AS label_kuartal,
    SUM(omzet)                           AS omzet_kuartal,
    SUM(jumlah_transaksi)                AS total_transaksi,
    ROUND(AVG(rata_transaksi)::numeric, 0) AS avg_nilai_transaksi,
    ROUND(
        (SUM(omzet) - LAG(SUM(omzet)) OVER (ORDER BY
            EXTRACT(YEAR FROM periode::date),
            EXTRACT(QUARTER FROM periode::date)
        )) / NULLIF(
            LAG(SUM(omzet)) OVER (ORDER BY
                EXTRACT(YEAR FROM periode::date),
                EXTRACT(QUARTER FROM periode::date)
            ), 0
        ) * 100, 2
    )                                    AS qoq_growth_pct
FROM tren_bulanan
GROUP BY tahun, kuartal
ORDER BY tahun, kuartal;
```

Catat hasil pada **Tabel B.2**.

---

### Langkah B.5 — Query summary keseluruhan platform

```sql
SELECT
    MIN(periode)                          AS periode_awal,
    MAX(periode)                          AS periode_akhir,
    SUM(omzet)                            AS total_omzet,
    SUM(jumlah_transaksi)                 AS total_transaksi,
    ROUND(AVG(rata_transaksi)::numeric,0) AS avg_nilai_per_transaksi,
    ROUND(
        (MAX(omzet) - MIN(omzet))::numeric
        / NULLIF(MIN(omzet), 0) * 100, 2
    )                                     AS variasi_omzet_pct
FROM tren_bulanan;
```

Catat nilai pada **Tabel B.2**.

---

## Bagian C — Pertanyaan Diskusi Konseptual

Jawab pertanyaan berikut berdasarkan teori modul dan pengalaman praktikum Latihan 1–5.

---

**C.1 — Pada Latihan 4, CPU usage meningkat saat Spark berjalan. Apakah peningkatan ini linier terhadap jumlah data yang diproses? Faktor apa yang mempengaruhi hubungan antara ukuran data dan utilisasi CPU pada Spark di mode YARN?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**C.2 — Jika pada dashboard Grafana Anda melihat `load average` yang terus-menerus di atas jumlah CPU core yang tersedia (misalnya load = 6 pada mesin dengan 4 core), apa implikasinya terhadap performa pipeline Spark yang berjalan di node yang sama? Langkah apa yang bisa diambil untuk mengatasi ini?**

> Petunjuk: Load average > jumlah core berarti proses mengantri menunggu CPU. Spark membagi pekerjaan ke beberapa task yang berjalan paralel — jika CPU tidak cukup, task-task ini akan bergantian (context switching).
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**C.3 — Bandingkan pendekatan visualisasi Superset dan Grafana dari tiga dimensi berikut: (a) tipe data yang divisualisasikan, (b) pengguna yang dituju, dan (c) frekuensi refresh data. Berikan contoh nyata dari praktikum ini untuk setiap poin.**

> | Dimensi | Apache Superset | Grafana |
> |---|---|---|
> | Tipe data | _..._ | _..._ |
> | Pengguna dituju | _..._ | _..._ |
> | Frekuensi refresh | _..._ | _..._ |
>
> Tulis penjelasan Anda di sini:
>
> _..._

---

**C.4 — Pada Superset, saat Anda menerapkan filter `Time Range`, hanya chart yang memiliki kolom temporal terhubung yang merespons. Bagaimana Anda akan mendesain skema tabel di PostgreSQL agar tabel `omzet_kategori` dan `omzet_kota` juga bisa difilter berdasarkan periode waktu — tanpa harus membuat ulang semua chart?**

> Petunjuk: Tambahkan kolom `periode` (atau `tahun` dan `bulan`) ke tabel `omzet_kategori` dan `omzet_kota`, sehingga agregasi dilakukan per periode, bukan secara keseluruhan.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**C.5 — Jika perusahaan ingin menambahkan monitoring khusus untuk Spark — misalnya durasi setiap Spark job, jumlah task yang gagal, atau throughput event per detik — komponen apa yang perlu ditambahkan ke arsitektur yang sudah ada? Gambarkan aliran data dari Spark hingga ke dashboard Grafana.**

> Petunjuk: Spark memiliki Metrics System bawaan yang bisa dikonfigurasi untuk mengirim metrik ke Prometheus menggunakan `PrometheusServlet` atau `spark-prometheus` library. Selain itu, Spark History Server menyimpan event log yang bisa di-parse.
>
> Gambarkan alur berikut dan lengkapi komponen yang hilang:
> ```
> Spark Job → [???] → Prometheus → Grafana
> ```
>
> Tulis jawaban Anda di sini:
>
> _..._

---

## Tabel Pencatatan Hasil

### Tabel A.1 — Hasil Query PromQL Lanjutan

| Query | Nilai / Temuan | Satuan | Interpretasi |
|---|---|---|---|
| `{job="node-exporter"}` — jumlah metrik total | _..._ | metrik | _..._ |
| Disk usage root filesystem (`/`) | _..._ | % | Aman / Perlu perhatian |
| Network receive (interface utama) | _..._ | KB/s | _..._ |
| Network transmit (interface utama) | _..._ | KB/s | _..._ |
| Uptime sistem | _..._ | jam | _..._ |
| Memori digunakan saat ini | _..._ | GB | _..._ |
| Total RAM host | _..._ | GB | _..._ |

**Filesystem dengan disk usage tertinggi:**

| Mountpoint | Fstype | Ukuran Total | Digunakan | Tersisa | Usage (%) |
|---|---|---|---|---|---|
| _..._ | _..._ | _..._ GB | _..._ GB | _..._ GB | _..._ % |
| _..._ | _..._ | _..._ GB | _..._ GB | _..._ GB | _..._ % |

### Tabel B.1 — Hasil Query Analisis Kota Lanjutan (Top 5)

| Kota | Total Omzet (Rp) | Total Transaksi | Total Pelanggan | Avg/Transaksi (Rp) | Avg/Pelanggan (Rp) | Frekuensi Belanja |
|---|---|---|---|---|---|---|
| _..._ | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |

**Temuan menarik dari analisis kota:**

| Temuan | Detail |
|---|---|
| Kota dengan avg/transaksi tertinggi | _..._ (Rp _..._) |
| Kota dengan frekuensi belanja tertinggi | _..._ (_..._ kali/pelanggan) |
| Apakah kota omzet terbesar = avg/transaksi terbesar? | Ya / Tidak |

### Tabel B.2 — Hasil Analisis Tren Kuartalan

| Kuartal | Omzet (Rp) | Total Transaksi | Avg Nilai/Trx (Rp) | QoQ Growth (%) | Keterangan |
|---|---|---|---|---|---|
| _...-Q1_ | _..._ | _..._ | _..._ | N/A | Kuartal pertama |
| _...-Q2_ | _..._ | _..._ | _..._ | _..._ % | _..._ |
| _...-Q3_ | _..._ | _..._ | _..._ | _..._ % | _..._ |
| _...-Q4_ | _..._ | _..._ | _..._ | _..._ % | _..._ |
| _...-Q1_ | _..._ | _..._ | _..._ | _..._ % | Tahun ke-2 |
| _...-Q2_ | _..._ | _..._ | _..._ | _..._ % | _..._ |
| _...-Q3_ | _..._ | _..._ | _..._ | _..._ % | _..._ |
| _...-Q4_ | _..._ | _..._ | _..._ | _..._ % | Kuartal terakhir |

**Summary keseluruhan platform (dari Query 5):**

| Informasi | Nilai |
|---|---|
| Periode data (awal) | _..._ |
| Periode data (akhir) | _..._ |
| Total omzet seluruh periode | Rp _..._ |
| Total transaksi | _..._ |
| Avg nilai per transaksi | Rp _..._ |
| Variasi omzet min–max (%) | _..._ % |

---

## Tabel Rangkuman Seluruh Latihan Modul 10

### Komponen Infrastruktur yang Berhasil Dikonfigurasi

| Komponen | Port | Fungsi | Status |
|---|---|---|---|
| PostgreSQL | 5432 | Backend Superset + storage data analitik | ☐ OK |
| Apache Superset | 8088 | BI dashboard untuk data bisnis e-commerce | ☐ OK |
| Prometheus | 9090 | Time-series metrics store + scraping | ☐ OK |
| Node Exporter | 9100 | Pengumpul metrik OS host | ☐ OK |
| Grafana | 3000 | Dashboard monitoring infrastruktur | ☐ OK |

### Alur Data End-to-End Modul 10

```
Transaksi E-Commerce (raw)
        ↓  [Spark: buat_data_ecommerce.py]
HDFS Silver/Bronze (12.000 transaksi + 300 pelanggan)
        ↓  [Spark: persiapan_data_analitik.py]
HDFS Gold (tren_bulanan, omzet_kategori, omzet_kota)
        ↓  [Spark JDBC: ekspor_ke_postgresql.py]
PostgreSQL analytics DB
        ↓  [Apache Superset]
Dashboard Bisnis (Bar Chart, Line Chart, Big Number, Table)

Host OS (CPU, Memory, Disk, Network)
        ↓  [Node Exporter]
Prometheus (scrape setiap 15 detik)
        ↓  [Grafana]
Dashboard Monitoring (Gauge CPU/Mem, Time Series Load)
```

### Chart dan Dashboard yang Berhasil Dibuat

| Latihan | Artefak | Tipe | Tool |
|---|---|---|---|
| 2 | Omzet per Kategori | Bar Chart | Superset |
| 2 | Tren Omzet Bulanan | Line Chart | Superset |
| 2 | Total Omzet Keseluruhan | Big Number | Superset |
| 2 | Top 10 Kota | Table | Superset |
| 3 | Analitik Platform E-Commerce | Dashboard (4 chart) | Superset |
| 4 | CPU Usage (%) | Gauge | Grafana |
| 4 | Memory Usage (%) | Gauge | Grafana |
| 4 | CPU Usage per Mode | Time Series | Grafana |
| 4 | System Load Average | Time Series | Grafana |
| 5 | Rata-rata Nilai Transaksi per Kota | Bar Chart | Superset |

---

## Kesimpulan Latihan 5

Setelah menyelesaikan seluruh rangkaian latihan Modul 10, lengkapi pernyataan berikut:

> "Dari eksplorasi PromQL lanjutan, ditemukan bahwa filesystem root menggunakan **___**% kapasitas disk dan memori host yang terpakai adalah **___** GB dari total **___** GB. Query analisis kota menunjukkan bahwa kota dengan `avg_nilai_per_transaksi` tertinggi adalah **___** (Rp **___**), yang **___** (sama/berbeda) dengan kota ber-omzet total tertinggi. Analisis kuartalan menunjukkan QoQ growth tertinggi terjadi pada kuartal **___** sebesar **___**%."

> "Perbedaan utama antara Superset dan Grafana: Superset ditujukan untuk **___** yang bekerja dengan **___** (data historis bisnis/metrik sistem real-time), sedangkan Grafana ditujukan untuk **___** yang membutuhkan **___** dengan refresh interval **___** (menit/detik). Keduanya saling melengkapi dalam ekosistem observabilitas Big Data — Superset menjawab pertanyaan '**___**' sementara Grafana menjawab pertanyaan '**___**'."

---

## Penutup Modul 10

Selamat! Anda telah menyelesaikan seluruh rangkaian latihan Modul 10. Berikut ringkasan pencapaian:

| Latihan | Topik Utama | Status |
|---|---|---|
| Latihan 1 | Setup 5 layanan Docker, generate data Spark, ekspor ke PostgreSQL | ☐ Selesai |
| Latihan 2 | Koneksi Superset–PostgreSQL, 4 visualisasi bisnis (Bar, Line, Big Number, Table) | ☐ Selesai |
| Latihan 3 | Dashboard interaktif, Native Filter, SQL Lab, 3 query analitik | ☐ Selesai |
| Latihan 4 | Dashboard Grafana, 4 panel monitoring, korelasi Spark vs CPU/Memory | ☐ Selesai |
| Latihan 5 | PromQL lanjutan (disk, network, uptime), SQL Lab lanjutan, 5 diskusi konseptual | ☐ Selesai |

---

*Modul 10 — Monitoring, Visualisasi, dan Eksplorasi Big Data · Institut Teknologi Sumatera*