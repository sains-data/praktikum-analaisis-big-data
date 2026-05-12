# Latihan 4 — Dashboard Monitoring Infrastruktur Grafana
**Modul 10 · Monitoring, Visualisasi, dan Eksplorasi Big Data** | Estimasi waktu: **20 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Memverifikasi koneksi datasource Prometheus di Grafana
- Membuat empat panel monitoring: Gauge CPU, Gauge Memory, Time Series CPU per mode, dan Time Series Load Average
- Menulis query PromQL untuk mengukur utilisasi CPU, memori, dan beban sistem
- Mengonfigurasi threshold warna pada panel Gauge
- Mengamati lonjakan metrik secara real-time saat Spark job dijalankan dan mengkorelasikannya dengan proses komputasi

---

## Prasyarat

- [ ] Latihan 1, 2, dan 3 selesai
- [ ] `modul10-prometheus` dan `modul10-grafana` berstatus `Up`
- [ ] `modul10-node-exporter` berstatus `Up` dan `node_cpu_seconds_total` tersedia di `http://localhost:9100/metrics`
- [ ] Prometheus Targets `http://localhost:9090/targets` — kedua target berstatus **UP**

---

## Langkah Kerja

### Langkah 4.1 — Verifikasi datasource Prometheus di Grafana

1. Login ke Grafana di `http://localhost:3000` (`admin` / `admin`)
2. Navigasi ke **Connections → Data Sources**
3. Klik **Prometheus** (seharusnya sudah terdaftar otomatis melalui provisioning)
4. Scroll ke bawah halaman, klik tombol **Save & Test**
5. Pastikan muncul banner hijau: *"Successfully queried the Prometheus API."*

Jika Prometheus belum terdaftar, tambahkan manual:
- Klik **+ Add new data source** → pilih **Prometheus**
- URL: `http://prometheus:9090`
- Klik **Save & Test**

Catat status pada **Tabel 4.1**.

---

### Langkah 4.2 — Buat dashboard baru

1. Navigasi ke **Dashboards** di sidebar kiri → klik **New** → **New Dashboard**
2. Akan muncul kanvas kosong dengan tombol **+ Add visualization**
3. Klik **Add visualization**
4. Pilih datasource: **Prometheus**

Jendela editor panel terbuka. Anda akan membuat 4 panel secara berurutan.

---

### Langkah 4.3 — Buat Panel 1: Gauge CPU Usage

Di editor panel yang sudah terbuka:

**Query PromQL (ketik di kolom Metrics browser):**

```promql
100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)
```

**Konfigurasi panel (panel kanan):**

| Setting | Nilai |
|---|---|
| Visualization Type | **Gauge** |
| Title | `CPU Usage (%)` |
| Description | `Persentase utilisasi CPU rata-rata` |
| Unit | `Percent (0-100)` |
| Min | `0` |
| Max | `100` |

**Konfigurasi Threshold (scroll ke bawah di panel kanan):**

Klik **+ Add threshold** dan atur:

| Nilai | Warna |
|---|---|
| 0 (Base) | Hijau (Green) |
| 60 | Kuning (Yellow) |
| 80 | Merah (Red) |

Klik **Apply** (pojok kanan atas editor).

**Catat nilai CPU Usage saat ini pada Tabel 4.2.**

---

### Langkah 4.4 — Buat Panel 2: Gauge Memory Usage

Di kanvas dashboard, klik **Add panel** → **Add visualization** → pilih Prometheus.

**Query PromQL:**

```promql
(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100
```

**Konfigurasi panel:**

| Setting | Nilai |
|---|---|
| Visualization Type | **Gauge** |
| Title | `Memory Usage (%)` |
| Description | `Persentase memori yang sedang digunakan` |
| Unit | `Percent (0-100)` |
| Min | `0` |
| Max | `100` |

**Threshold:** sama seperti Panel 1 (0=Hijau, 60=Kuning, 80=Merah)

Klik **Apply**.

**Catat nilai Memory Usage saat ini pada Tabel 4.2.**

---

### Langkah 4.5 — Buat Panel 3: Time Series CPU per Mode

Klik **Add panel** → **Add visualization** → pilih Prometheus.

**Query PromQL:**

```promql
rate(node_cpu_seconds_total[5m]) * 100
```

Di bagian **Options** (bawah query), atur:

| Setting | Nilai |
|---|---|
| Legend | `{{mode}}` |

Klik **+ Add query** untuk menambahkan query kedua yang lebih spesifik (hanya mode penting):

```promql
rate(node_cpu_seconds_total{mode=~"user|system|iowait"}[5m]) * 100
```

Hapus query pertama (klik ikon tempat sampah di sebelah kiri query). Gunakan hanya query kedua ini.

**Konfigurasi panel:**

| Setting | Nilai |
|---|---|
| Visualization Type | **Time Series** |
| Title | `CPU Usage per Mode` |
| Unit | `Percent (0-100)` |
| Legend Mode | `Table` |
| Legend Placement | `Bottom` |
| Fill Opacity | `10` |

Klik **Apply**.

---

### Langkah 4.6 — Buat Panel 4: Time Series Load Average

Klik **Add panel** → **Add visualization** → pilih Prometheus.

Tambahkan **tiga query** secara berurutan (klik **+ Add query** untuk setiap query tambahan):

**Query A:**
```promql
node_load1
```
Di Legend: ketik `1 menit`

**Query B:**
```promql
node_load5
```
Di Legend: ketik `5 menit`

**Query C:**
```promql
node_load15
```
Di Legend: ketik `15 menit`

**Konfigurasi panel:**

| Setting | Nilai |
|---|---|
| Visualization Type | **Time Series** |
| Title | `System Load Average` |
| Description | `Rata-rata antrian proses yang menunggu CPU` |
| Unit | `Short` |
| Legend Mode | `Table` |
| Legend Placement | `Bottom` |

> **Cara membaca Load Average:** Nilai `node_load1 = 2.0` berarti rata-rata ada
> 2 proses yang menunggu atau menggunakan CPU dalam 1 menit terakhir. Jika
> host memiliki 4 CPU core, load average 2.0 berarti utilisasi ~50%.

Klik **Apply**.

---

### Langkah 4.7 — Atur layout dan simpan dashboard

Di kanvas dashboard, susun keempat panel:

```
┌──────────────────────┬──────────────────────┐
│   CPU Usage (%)      │   Memory Usage (%)    │  ← Baris 1: dua Gauge berdampingan
│      (Gauge)         │      (Gauge)          │
├──────────────────────┴──────────────────────┤
│          CPU Usage per Mode                  │  ← Baris 2: Time Series CPU
│           (Time Series)                      │
├─────────────────────────────────────────────┤
│          System Load Average                 │  ← Baris 3: Time Series Load
│           (Time Series)                      │
└─────────────────────────────────────────────┘
```

Cara mengatur layout:
- Seret panel dari sudut kanan bawah untuk mengubah ukuran
- Seret dari header panel untuk memindahkan posisi
- Untuk dua Gauge berdampingan: drag panel Memory ke sebelah kanan panel CPU

Klik **Save Dashboard** → isi nama: `BigData Infrastructure Monitoring` → klik **Save**.

---

### Langkah 4.8 — Atur time range dan auto-refresh

Di pojok kanan atas dashboard Grafana:

1. Klik dropdown time range (menampilkan `Last 6 hours` atau sejenisnya)
2. Ubah ke **Last 15 minutes**
3. Aktifkan auto-refresh: klik dropdown di sebelah kanan time range → pilih **10s**

Grafana sekarang akan me-refresh semua panel setiap 10 detik secara otomatis.

---

### Langkah 4.9 — Catat baseline metrik sebelum beban

Amati nilai panel saat sistem dalam kondisi **idle** (tidak ada beban berat). Catat nilai pada **Tabel 4.2** di kolom "Sebelum Spark".

Metrik yang dicatat:
- CPU Usage (%) — nilai dari Gauge
- Memory Usage (%) — nilai dari Gauge
- Load Average 1 menit — nilai dari panel Load Average
- Mode CPU yang dominan di Time Series (`idle`, `user`, atau `system`)

---

### Langkah 4.10 — Jalankan Spark untuk menghasilkan beban

Buka terminal baru. Masuk ke kontainer `bigdata-spark`:

```bash
bash login.sh   # dari direktori bigdata-spark/
```

Jalankan skrip agregasi Spark (ini akan menghasilkan beban CPU dan memori):

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  /tmp/persiapan_data_analitik.py
```

**Segera setelah perintah dijalankan**, pindah ke browser dan pantau dashboard Grafana. Amati perubahan pada setiap panel selama proses berlangsung (~3–5 menit).

**Yang diamati secara real-time:**
- Kapan CPU Usage mulai naik setelah `spark-submit` dieksekusi?
- Apakah kenaikan bertahap atau langsung melonjak?
- Mode CPU mana yang naik paling signifikan: `user`, `system`, atau `iowait`?
- Apakah Memory Usage ikut naik saat Spark berjalan?
- Setelah Spark selesai, berapa lama hingga CPU kembali ke baseline?

Catat semua pengamatan pada **Tabel 4.2** di kolom "Saat Spark Berjalan" dan "Setelah Spark Selesai".

---

### Langkah 4.11 — Verifikasi query di Prometheus Expression Browser

Buka `http://localhost:9090/graph` dan jalankan beberapa query untuk memverifikasi data:

**Query 1 — Total RAM dalam GB:**
```promql
node_memory_MemTotal_bytes / 1024 / 1024 / 1024
```

**Query 2 — RAM tersedia dalam GB:**
```promql
node_memory_MemAvailable_bytes / 1024 / 1024 / 1024
```

**Query 3 — CPU idle rate:**
```promql
avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100
```

Catat hasil setiap query pada **Tabel 4.3**.

---

## Tabel Pencatatan Hasil

### Tabel 4.1 — Status Konfigurasi Grafana

| Item | Status | Keterangan |
|---|---|---|
| Datasource Prometheus terdaftar | Ya / Tidak | _..._ |
| Save & Test Prometheus berhasil | Ya / Tidak | _..._ |
| Panel CPU Usage (Gauge) dibuat | Ya / Tidak | _..._ |
| Panel Memory Usage (Gauge) dibuat | Ya / Tidak | _..._ |
| Panel CPU Usage per Mode (Time Series) dibuat | Ya / Tidak | _..._ |
| Panel System Load Average (Time Series) dibuat | Ya / Tidak | _..._ |
| Dashboard `BigData Infrastructure Monitoring` tersimpan | Ya / Tidak | _..._ |
| Auto-refresh 10 detik aktif | Ya / Tidak | _..._ |

### Tabel 4.2 — Pengamatan Metrik: Idle vs Spark Berjalan vs Recovery

| Metrik | Sebelum Spark (Idle) | Saat Spark Berjalan (Puncak) | Setelah Spark Selesai | Selisih Puncak–Idle |
|---|---|---|---|---|
| CPU Usage (%) | _..._ % | _..._ % | _..._ % | _..._ % |
| Memory Usage (%) | _..._ % | _..._ % | _..._ % | _..._ % |
| Load Average 1 menit | _..._ | _..._ | _..._ | _..._ |
| Load Average 5 menit | _..._ | _..._ | _..._ | _..._ |
| Mode CPU dominan | _..._ | _..._ | _..._ | — |
| Durasi lonjakan CPU | — | _..._ detik | — | — |
| Waktu recovery ke baseline | — | — | _..._ detik | — |

**Waktu mulai Spark:** _HH:MM:SS_
**Waktu Spark selesai:** _HH:MM:SS_
**Durasi total Spark job:** _..._ menit _..._ detik

### Tabel 4.3 — Hasil Query Prometheus Expression Browser

| Query PromQL | Nilai yang Dikembalikan | Satuan | Waktu Query |
|---|---|---|---|
| `node_memory_MemTotal_bytes / 1024 / 1024 / 1024` | _..._ | GB | _HH:MM_ |
| `node_memory_MemAvailable_bytes / 1024 / 1024 / 1024` | _..._ | GB | _HH:MM_ |
| `avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100` | _..._ % | % | _HH:MM_ |
| **Memori yang digunakan (Total − Available)** | _..._ | GB | — |
| **Memori yang digunakan (%)** | _..._ % | % | — |

### Tabel 4.4 — Deskripsi Setiap Panel Dashboard

*(isi berdasarkan pengamatan di Grafana)*

| Panel | Visualization Type | Query PromQL | Rentang Nilai Saat Ini | Warna Threshold Aktif |
|---|---|---|---|---|
| CPU Usage (%) | Gauge | `100 - (avg(rate(...idle...)) * 100)` | _..._ % | Hijau / Kuning / Merah |
| Memory Usage (%) | Gauge | `(1 - MemAvailable/MemTotal) * 100` | _..._ % | Hijau / Kuning / Merah |
| CPU Usage per Mode | Time Series | `rate(node_cpu_seconds_total...)` | _..._ | — |
| System Load Average | Time Series | `node_load1`, `node_load5`, `node_load15` | _..._ | — |

---

## Refleksi dan Analisis

**R4.1 — Dari Tabel 4.2, perhatikan jeda waktu antara perintah `spark-submit` dijalankan dan momen CPU Usage mulai melonjak di Grafana. Mengapa ada jeda ini? Jelaskan apa yang terjadi dalam proses bootstrapping Spark (driver start, YARN negotiation, executor launch) selama jeda tersebut.**

> Petunjuk: Spark tidak langsung memproses data begitu `spark-submit` dijalankan — ada negosiasi resource dengan YARN, kemudian executor perlu diluncurkan dan JVM perlu diinisialisasi.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.2 — Dari Tabel 4.2, mode CPU mana yang paling dominan naik saat Spark berjalan: `user`, `system`, atau `iowait`? Jelaskan makna teknis dari setiap mode dan mengapa mode tersebut naik saat Spark melakukan komputasi (bukan I/O-intensive).**

> Definisi:
> - `user`: waktu CPU untuk proses user-space (JVM Spark, Python)
> - `system`: waktu CPU untuk operasi kernel (network, filesystem)
> - `iowait`: waktu CPU menunggu operasi I/O selesai (disk read/write ke HDFS)
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.3 — Dari Tabel 4.3, hitung selisih antara `MemTotal` dan `MemAvailable`. Apakah selisih ini sama dengan `Memory Usage` yang ditampilkan di panel Gauge? Mengapa formula `1 - (MemAvailable / MemTotal)` lebih akurat untuk mengukur utilisasi memori aktual dibandingkan `(MemTotal - MemFree) / MemTotal`?**

> Petunjuk: `MemFree` adalah memori yang benar-benar kosong, sedangkan `MemAvailable` sudah memperhitungkan memori yang bisa dibebaskan dari page cache dan buffer. Di Linux, page cache tidak selalu "terpakai" secara nyata.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.4 — Dari Tabel 4.2, berapa lama CPU kembali ke baseline setelah Spark selesai? Jika dalam skenario produksi ada pipeline Spark yang berjalan setiap 15 menit, dan recovery membutuhkan 5 menit, apa implikasi terhadap ketersediaan sumber daya untuk pipeline berikutnya?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.5 — Dashboard Grafana yang Anda buat menggunakan data dari Node Exporter yang mengumpulkan metrik OS host. Jika Spark berjalan di klaster multi-node (10 worker node), komponen apa yang perlu ditambahkan agar semua node terpantau di satu dashboard Grafana? Bagaimana query PromQL harus dimodifikasi untuk mengagregasi metrik dari semua node?**

> Petunjuk: Setiap node worker membutuhkan instance Node Exporter sendiri. Prometheus perlu dikonfigurasi untuk scrape semua instance tersebut. Query PromQL bisa menggunakan label `instance` untuk memfilter atau mengagregasi.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 4

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Dashboard `BigData Infrastructure Monitoring` berhasil dibuat dengan **___** panel menggunakan datasource **___**. Saat kondisi idle, CPU Usage berada di **___**% dan Memory Usage di **___**%. Saat Spark berjalan, CPU Usage melonjak hingga **___**% dengan mode yang paling dominan naik adalah **___** — ini mengindikasikan bahwa Spark terutama melakukan komputasi di **___** (user-space/kernel-space). Load average 1 menit mencapai puncak **___** yang setara dengan **___**% utilisasi dari total **___** CPU core tersedia. CPU kembali ke baseline dalam waktu sekitar **___** detik setelah job selesai."

---

*Latihan 4 selesai. Lanjutkan ke **Latihan 5 — Eksplorasi Lanjutan: PromQL, SQL Lab, dan Diskusi**.*