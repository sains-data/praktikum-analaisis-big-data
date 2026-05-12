# Latihan 2 — Eksplorasi Data dan Visualisasi Apache Superset
**Modul 10 · Monitoring, Visualisasi, dan Eksplorasi Big Data** | Estimasi waktu: **25 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Mendaftarkan koneksi database PostgreSQL di Apache Superset
- Mendaftarkan tabel analitik sebagai Dataset yang siap divisualisasikan
- Membuat empat jenis visualisasi: Bar Chart, Line Chart, Big Number, dan Table
- Mengonfigurasi setiap chart dengan metrik, dimensi, dan pengurutan yang tepat
- Membaca dan menginterpretasikan hasil visualisasi dari perspektif bisnis e-commerce

---

## Prasyarat

- [ ] Latihan 1 selesai — semua 5 container berjalan
- [ ] Tabel `tren_bulanan`, `omzet_kategori`, `omzet_kota` tersedia di PostgreSQL
- [ ] Superset dapat diakses di `http://localhost:8088`

---

## Langkah Kerja

### Langkah 2.1 — Daftarkan koneksi database di Superset

1. Login ke Superset di `http://localhost:8088` dengan `admin / admin`
2. Klik **Settings** (ikon roda gigi, pojok kanan atas) → **Database Connections**
3. Klik tombol **+ Database** → pilih **PostgreSQL** dari daftar
4. Isi formulir koneksi:

   | Field | Nilai |
   |---|---|
   | Display Name | `Analitik E-Commerce` |
   | Host | `postgres` |
   | Port | `5432` |
   | Database Name | `analytics` |
   | Username | `superset` |
   | Password | `superset` |

5. Klik **Test Connection** — pastikan muncul banner hijau *"Connection looks good!"*
6. Klik **Connect**

> Jika Test Connection gagal dengan error *"could not connect"*, periksa apakah
> `modul10-postgres` berstatus `Up (healthy)` di `docker compose ps`.

Catat hasil pada **Tabel 2.1**.

---

### Langkah 2.2 — Daftarkan ketiga tabel sebagai Dataset

Navigasi ke **Data → Datasets → + Dataset** (tombol biru kanan atas).

Lakukan tiga kali untuk mendaftarkan setiap tabel:

**Dataset 1:**
- Database: `Analitik E-Commerce`
- Schema: `public`
- Table: `tren_bulanan`
- Klik **Add Dataset and Create Chart** → tutup halaman chart yang terbuka

**Dataset 2:**
- Database: `Analitik E-Commerce`
- Schema: `public`
- Table: `omzet_kategori`
- Klik **Add Dataset and Create Chart** → tutup

**Dataset 3:**
- Database: `Analitik E-Commerce`
- Schema: `public`
- Table: `omzet_kota`
- Klik **Add Dataset and Create Chart** → tutup

Verifikasi: buka **Data → Datasets** — pastikan ketiga dataset muncul dalam daftar.

Catat pada **Tabel 2.1**.

---

### Langkah 2.3 — Buat visualisasi pertama: Bar Chart Omzet per Kategori

1. Di halaman **Data → Datasets**, klik ikon **Explore** (ikon grafik) pada baris `omzet_kategori`
2. Di halaman Explore, klik **Chart Type** dan pilih **Bar Chart**
3. Konfigurasi di panel kiri:

   | Setting | Nilai |
   |---|---|
   | Metrics | `SUM(omzet_total)` |
   | Dimensions | `kategori` |
   | Sort By | `omzet_total` Descending |
   | Row Limit | 20 |
   | Show Legend | aktifkan |

4. Klik **Update Chart** (tombol biru di bawah konfigurasi)
5. Amati chart yang muncul, lalu klik **Save** → isi nama: `Omzet per Kategori` → klik **Save**

**Yang diamati:**
- Urutan kategori dari omzet tertinggi ke terendah
- Perbedaan tinggi bar antar kategori (seberapa besar selisihnya)
- Apakah ada kategori yang sangat mendominasi?

Catat pada **Tabel 2.2**.

---

### Langkah 2.4 — Buat visualisasi kedua: Line Chart Tren Omzet Bulanan

1. Klik **Explore** pada dataset `tren_bulanan`
2. Pilih Chart Type: **Line Chart**
3. Konfigurasi:

   | Setting | Nilai |
   |---|---|
   | Time Column | `periode` |
   | Time Grain | Month |
   | Metrics | `SUM(omzet)` (tambahkan juga `AVG(ma3_omzet)`) |
   | Contribution Mode | Off |
   | Show Legend | aktifkan |
   | X Axis Label | `Periode (Bulan)` |
   | Y Axis Label | `Omzet (Rp)` |

4. Untuk menambahkan metrik kedua (`AVG(ma3_omzet)`): klik **+ Add Metric** di bagian Metrics, pilih `ma3_omzet`, fungsi agregasi `AVG`
5. Klik **Update Chart**
6. Simpan dengan nama: `Tren Omzet Bulanan`

**Yang diamati:**
- Tren umum: apakah omzet naik, turun, atau fluktuatif?
- Pola musiman: apakah ada bulan tertentu yang selalu lebih tinggi?
- Moving average (`ma3_omzet`) lebih halus atau lebih tajam dari omzet aktual?
- Bulan dengan omzet tertinggi dan terendah

Catat pada **Tabel 2.3**.

---

### Langkah 2.5 — Buat visualisasi ketiga: Big Number Total Omzet

1. Klik **Explore** pada dataset `tren_bulanan`
2. Pilih Chart Type: **Big Number with Trendline**
3. Konfigurasi:

   | Setting | Nilai |
   |---|---|
   | Metric | `SUM(omzet)` |
   | Time Column | `periode` |
   | Subheader | `Total Omzet Platform E-Commerce` |

4. Klik **Update Chart** — akan muncul satu angka besar total omzet dengan grafik tren kecil di bawahnya
5. Simpan dengan nama: `Total Omzet Keseluruhan`

Catat nilai total omzet yang ditampilkan pada **Tabel 2.2**.

---

### Langkah 2.6 — Buat visualisasi keempat: Tabel Top Kota

1. Klik **Explore** pada dataset `omzet_kota`
2. Pilih Chart Type: **Table**
3. Konfigurasi:

   | Setting | Nilai |
   |---|---|
   | Columns | `kota`, `omzet`, `transaksi`, `pelanggan_unik` |
   | Sort By | `omzet` Descending |
   | Row Limit | 10 |
   | Conditional Formatting | aktifkan untuk kolom `omzet` (color scale) |

4. Klik **Update Chart**
5. Simpan dengan nama: `Top 10 Kota`

**Yang diamati:**
- Kota dengan omzet tertinggi dan terendah di top-10
- Apakah kota dengan transaksi terbanyak selalu memiliki omzet tertinggi?
- Selisih antara kota peringkat 1 dan peringkat 10
- Rata-rata `pelanggan_unik` di kota-kota top

Catat pada **Tabel 2.4**.

---

### Langkah 2.7 — Verifikasi semua chart tersimpan

Navigasi ke **Charts** di menu atas. Pastikan keempat chart ini muncul:

| Nama Chart | Tipe | Dataset |
|---|---|---|
| `Omzet per Kategori` | Bar Chart | omzet_kategori |
| `Tren Omzet Bulanan` | Line Chart | tren_bulanan |
| `Total Omzet Keseluruhan` | Big Number with Trendline | tren_bulanan |
| `Top 10 Kota` | Table | omzet_kota |

Catat konfirmasi pada **Tabel 2.1**.

---

## Tabel Pencatatan Hasil

### Tabel 2.1 — Status Konfigurasi Superset

| Item | Status | Keterangan |
|---|---|---|
| Database Connection "Analitik E-Commerce" | Terdaftar / Gagal | _..._ |
| Test Connection PostgreSQL | Sukses / Gagal | _..._ |
| Dataset `tren_bulanan` | Terdaftar / Gagal | _..._ |
| Dataset `omzet_kategori` | Terdaftar / Gagal | _..._ |
| Dataset `omzet_kota` | Terdaftar / Gagal | _..._ |
| Chart `Omzet per Kategori` | Tersimpan / Gagal | _..._ |
| Chart `Tren Omzet Bulanan` | Tersimpan / Gagal | _..._ |
| Chart `Total Omzet Keseluruhan` | Tersimpan / Gagal | _..._ |
| Chart `Top 10 Kota` | Tersimpan / Gagal | _..._ |

### Tabel 2.2 — Hasil Bar Chart: Omzet per Kategori

*(urutan dari tertinggi ke terendah)*

| Rank | Kategori | Total Omzet (Rp) | % dari Total | Catatan |
|---|---|---|---|---|
| 1 | _..._ | _..._ | _..._ | _..._ |
| 2 | _..._ | _..._ | _..._ | _..._ |
| 3 | _..._ | _..._ | _..._ | _..._ |
| 4 | _..._ | _..._ | _..._ | _..._ |
| 5 | _..._ | _..._ | _..._ | _..._ |
| 6 | _..._ | _..._ | _..._ | _..._ |
| **Total Keseluruhan** (Big Number) | — | **Rp _..._** | 100% | — |

**Selisih omzet antara kategori tertinggi dan terendah:** Rp _..._

### Tabel 2.3 — Hasil Line Chart: Tren Omzet Bulanan

*(catat nilai dari chart)*

| Bulan | Omzet Aktual (Rp) | MA3 Omzet (Rp) | MoM Growth (%) | Keterangan |
|---|---|---|---|---|
| _bulan ke-1_ | _..._ | _..._ | N/A | Awal periode |
| _bulan ke-6_ | _..._ | _..._ | _..._ | Pertengahan |
| _bulan ke-12_ | _..._ | _..._ | _..._ | _..._ |
| _bulan ke-18_ | _..._ | _..._ | _..._ | _..._ |
| _bulan ke-24_ | _..._ | _..._ | _..._ | Akhir periode |

**Ringkasan tren:**

| Informasi | Nilai |
|---|---|
| Bulan dengan omzet tertinggi | _..._ (Rp _..._) |
| Bulan dengan omzet terendah | _..._ (Rp _..._) |
| Tren umum (naik/turun/fluktuatif) | _..._ |
| Apakah MA3 lebih halus dari omzet aktual? | Ya / Tidak |

### Tabel 2.4 — Hasil Tabel: Top 10 Kota

| Rank | Kota | Omzet (Rp) | Transaksi | Pelanggan Unik | Avg Transaksi/Pelanggan |
|---|---|---|---|---|---|
| 1 | _..._ | _..._ | _..._ | _..._ | _..._ |
| 2 | _..._ | _..._ | _..._ | _..._ | _..._ |
| 3 | _..._ | _..._ | _..._ | _..._ | _..._ |
| 4 | _..._ | _..._ | _..._ | _..._ | _..._ |
| 5 | _..._ | _..._ | _..._ | _..._ | _..._ |
| 6–10 | _..._ | _..._ | _..._ | _..._ | _..._ |

**Apakah kota dengan transaksi terbanyak = kota dengan omzet tertinggi?** Ya / Tidak

---

## Refleksi dan Analisis

**R2.1 — Dari Tabel 2.2 (Bar Chart), apakah distribusi omzet antar kategori merata atau ada kategori yang sangat mendominasi? Jika satu kategori menyumbang lebih dari 40% total omzet, apa implikasi bisnisnya bagi perusahaan e-commerce? Apa risiko strategis dari ketergantungan tinggi pada satu kategori?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.2 — Dari Tabel 2.3 (Line Chart), perhatikan perbedaan antara garis omzet aktual dan garis MA3 (moving average 3 bulan). Mengapa garis MA3 tampak lebih halus? Kapan MA3 memberikan gambaran yang lebih berguna daripada omzet aktual mentah, dan kapan MA3 justru menyembunyikan informasi penting?**

> Petunjuk: MA3 adalah rata-rata 3 bulan sebelumnya. Bayangkan ada lonjakan omzet tiba-tiba di satu bulan — bagaimana MA3 merespons dibandingkan garis aktual?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.3 — Dari Tabel 2.4 (Top 10 Kota), jika kota A memiliki omzet tinggi tetapi `pelanggan_unik` rendah, apa yang dapat disimpulkan tentang karakteristik pelanggan di kota A? Bandingkan dengan kota B yang memiliki `pelanggan_unik` tinggi tetapi omzet tidak sebanding. Strategi pemasaran apa yang tepat untuk masing-masing?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.4 — Superset memisahkan konsep Dataset, Chart, dan Dashboard sebagai tiga entitas berbeda. Mengapa pemisahan ini penting dalam konteks tim BI yang besar, di mana satu dataset bisa digunakan oleh banyak chart, dan satu chart bisa muncul di banyak dashboard?**

> Petunjuk: Bayangkan skenario: nilai data di PostgreSQL berubah (karena ada transaksi baru). Apakah semua chart yang menggunakan dataset yang sama ikut ter-update, atau harus diperbarui satu per satu?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.5 — Kolom `ma3_omzet` dan `mom_growth_pct` dihitung oleh Spark (menggunakan Window function) dan disimpan ke PostgreSQL, bukan dihitung oleh Superset saat query. Apa keuntungan dan kerugian pendekatan "pre-computed metrics" ini dibandingkan menghitung metrik secara real-time di Superset menggunakan SQL kustom?**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 2

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Empat chart berhasil dibuat di Superset menggunakan data dari **___** tabel PostgreSQL. Kategori dengan omzet tertinggi adalah **___** (Rp **___**), menyumbang **___**% dari total. Kota dengan omzet tertinggi adalah **___**. Tren omzet bulanan menunjukkan pola yang **___** (naik/turun/fluktuatif) dengan omzet puncak pada bulan **___**. Moving average 3 bulan berfungsi untuk **___** dengan cara meratakan **___** jangka pendek pada data omzet."

---

*Latihan 2 selesai. Lanjutkan ke **Latihan 3 — Penyusunan Dashboard Interaktif Superset**.*