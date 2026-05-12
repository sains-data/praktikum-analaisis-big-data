# Latihan 3 — Penyusunan Dashboard Interaktif Superset
**Modul 10 · Monitoring, Visualisasi, dan Eksplorasi Big Data** | Estimasi waktu: **15 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Membuat dashboard baru di Apache Superset dan menyusun layout chart secara terstruktur
- Menambahkan Native Filter untuk eksplorasi data berbasis periode waktu
- Menguji interaktivitas dashboard melalui filter dan present mode
- Menggunakan SQL Lab untuk query analitik ad-hoc langsung ke PostgreSQL
- Menginterpretasikan dashboard sebagai alat komunikasi insight bisnis

---

## Prasyarat

- [ ] Latihan 2 selesai — keempat chart tersimpan di Superset
- [ ] Chart yang tersedia: `Omzet per Kategori`, `Tren Omzet Bulanan`, `Total Omzet Keseluruhan`, `Top 10 Kota`

---

## Langkah Kerja

### Langkah 3.1 — Buat dashboard baru

1. Navigasi ke **Dashboards** di menu atas
2. Klik tombol **+ Dashboard** (pojok kanan atas)
3. Isi nama: `Analitik Platform E-Commerce`
4. Klik **Save**
5. Setelah tersimpan, klik **Edit Dashboard** (tombol pensil di kanan atas)

---

### Langkah 3.2 — Susun chart pada dashboard

Mode edit dashboard menampilkan panel kiri berisi daftar semua chart tersimpan. Seret dan susun chart ke area kanvas sesuai layout berikut:

**Target layout:**

```
┌────────────────────────────────────────────┐
│          Total Omzet Keseluruhan           │  ← Baris 1, lebar penuh
│             (Big Number)                   │
├────────────────────────────────────────────┤
│           Tren Omzet Bulanan               │  ← Baris 2, lebar penuh
│             (Line Chart)                   │
├──────────────────────┬─────────────────────┤
│  Omzet per Kategori  │    Top 10 Kota      │  ← Baris 3, masing-masing setengah lebar
│    (Bar Chart)       │     (Table)         │
└──────────────────────┴─────────────────────┘
```

**Cara menyusun:**
- Seret chart dari panel kiri ke area kosong di kanvas
- Untuk mengatur lebar: geser handle pembatas antar chart ke kiri atau kanan
- Untuk mengatur tinggi: geser handle di bawah setiap chart

Setelah tersusun, klik **Save** (tombol kanan atas).

---

### Langkah 3.3 — Tambahkan Native Filter periode waktu

1. Di mode edit dashboard, klik ikon **Filter** (ikon corong di toolbar atas kanan) → **Add/Edit Filters**
2. Klik **+ Add Filter**
3. Konfigurasi filter:

   | Setting | Nilai |
   |---|---|
   | Filter Type | Time Range |
   | Filter Name | `Periode Analisis` |
   | Default Value | `Last year` |

4. Di bagian **Scope**, centang semua chart agar filter diterapkan ke seluruh dashboard
5. Klik **Save** (dalam modal filter) lalu **Save** (dashboard)

---

### Langkah 3.4 — Uji interaktivitas dashboard

Setelah tersimpan, keluar dari mode edit (klik tanda silang atau tombol **Cancel Edit**) dan masuk ke mode view.

**Uji 1 — Ubah filter periode:**

1. Klik dropdown filter `Periode Analisis` di atas dashboard
2. Ubah dari *Last year* ke **Last 6 months**
3. Amati: apakah chart `Tren Omzet Bulanan` dan `Total Omzet Keseluruhan` berubah?
4. Catat nilai Total Omzet saat filter **Last year** vs **Last 6 months** pada **Tabel 3.1**

**Uji 2 — Cross-filter antar chart:**

1. Klik salah satu bar pada chart `Omzet per Kategori` (misalnya klik bar "elektronik")
2. Amati: apakah chart lain merespons dengan memfilter hanya data kategori tersebut?
3. Klik lagi untuk menghapus filter
4. Catat pengamatan pada **Tabel 3.2**

**Uji 3 — Present Mode:**

1. Klik ikon **Present Mode** (ikon layar penuh, pojok kanan atas dashboard)
2. Amati tampilan fullscreen dashboard
3. Tekan `Escape` atau klik tombol keluar untuk kembali

---

### Langkah 3.5 — Eksplorasi dengan SQL Lab

SQL Lab memungkinkan query SQL ad-hoc langsung ke PostgreSQL, berguna untuk analisis yang belum ada chart-nya.

1. Navigasi ke **SQL Lab** di menu atas
2. Pilih database: `Analitik E-Commerce`
3. Pilih schema: `public`

**Query 1 — Analisis bulan terbaik per kategori:**

```sql
-- Lihat rata-rata omzet bulanan secara keseluruhan
SELECT
    periode,
    omzet,
    ma3_omzet,
    mom_growth_pct,
    ROUND(omzet::numeric / SUM(omzet) OVER () * 100, 2) AS persen_dari_total
FROM tren_bulanan
ORDER BY periode;
```

Klik **Run** dan amati hasilnya. Catat baris dengan `mom_growth_pct` tertinggi dan terendah pada **Tabel 3.3**.

**Query 2 — Kota dengan nilai rata-rata transaksi tertinggi:**

```sql
SELECT
    kota,
    omzet,
    transaksi,
    pelanggan_unik,
    ROUND(omzet::numeric / NULLIF(transaksi, 0), 0) AS avg_per_transaksi,
    ROUND(omzet::numeric / NULLIF(pelanggan_unik, 0), 0) AS avg_per_pelanggan
FROM omzet_kota
ORDER BY avg_per_transaksi DESC
LIMIT 10;
```

Catat top-5 kota berdasarkan `avg_per_transaksi` pada **Tabel 3.3**.

**Query 3 — Analisis persentase omzet per kategori:**

```sql
SELECT
    kategori,
    omzet_total,
    jumlah_transaksi,
    ROUND(omzet_rata::numeric, 0) AS avg_omzet_per_trx,
    persen_omzet,
    RANK() OVER (ORDER BY omzet_total DESC) AS ranking
FROM omzet_kategori
ORDER BY ranking;
```

Catat pada **Tabel 3.4**.

---

### Langkah 3.6 — Simpan query sebagai Saved Query

1. Setelah menjalankan Query 2, klik **Save** (pojok kanan atas SQL Lab)
2. Isi nama: `Analisis Kota Lanjutan`
3. Klik **Save**

Saved Query bisa diakses kembali melalui ikon bookmark di SQL Lab.

---

## Tabel Pencatatan Hasil

### Tabel 3.1 — Efek Filter Periode pada Dashboard

| Filter Aktif | Total Omzet | Jumlah Baris di Line Chart | Bulan Puncak |
|---|---|---|---|
| Last year (12 bulan) | Rp _..._ | _..._ bulan | _..._ |
| Last 6 months | Rp _..._ | _..._ bulan | _..._ |
| Selisih omzet | Rp _..._ | — | — |
| Apakah semua chart berubah? | Ya / Tidak | — | _..._ chart berubah |

### Tabel 3.2 — Pengamatan Interaktivitas Dashboard

| Aspek | Hasil Pengamatan |
|---|---|
| Apakah klik bar di `Omzet per Kategori` memfilter chart lain? | Ya / Tidak |
| Chart mana yang merespons cross-filter? | _..._ |
| Chart mana yang TIDAK merespons cross-filter? | _..._ |
| Apakah Present Mode menghilangkan menu navigasi atas? | Ya / Tidak |
| Apakah filter masih bisa digunakan di Present Mode? | Ya / Tidak |

### Tabel 3.3 — Hasil Query SQL Lab

**Query 1 — Growth Rate Bulanan (5 baris paling signifikan):**

| Periode | Omzet (Rp) | MoM Growth (%) | % dari Total | Keterangan |
|---|---|---|---|---|
| _..._ | _..._ | _..._ (tertinggi) | _..._ | _..._ |
| _..._ | _..._ | _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ (terendah/negatif) | _..._ | _..._ |

**Query 2 — Top 5 Kota berdasarkan Avg Nilai per Transaksi:**

| Rank | Kota | Avg per Transaksi (Rp) | Avg per Pelanggan (Rp) | Catatan |
|---|---|---|---|---|
| 1 | _..._ | _..._ | _..._ | _..._ |
| 2 | _..._ | _..._ | _..._ | _..._ |
| 3 | _..._ | _..._ | _..._ | _..._ |
| 4 | _..._ | _..._ | _..._ | _..._ |
| 5 | _..._ | _..._ | _..._ | _..._ |

### Tabel 3.4 — Hasil Query 3: Ranking Kategori Lengkap

| Rank | Kategori | Total Omzet (Rp) | Jumlah Transaksi | Avg Omzet/Trx (Rp) | % Omzet |
|---|---|---|---|---|---|
| 1 | _..._ | _..._ | _..._ | _..._ | _..._ |
| 2 | _..._ | _..._ | _..._ | _..._ | _..._ |
| 3 | _..._ | _..._ | _..._ | _..._ | _..._ |
| 4 | _..._ | _..._ | _..._ | _..._ | _..._ |
| 5 | _..._ | _..._ | _..._ | _..._ | _..._ |
| 6 | _..._ | _..._ | _..._ | _..._ | _..._ |

---

## Refleksi dan Analisis

**R3.1 — Dari Tabel 3.1, nilai Total Omzet saat filter "Last 6 months" adalah sekitar setengah dari "Last year". Apakah ini berarti bisnis stabil? Justifikasi jawaban Anda dengan membandingkan nilai MoM growth dari Query 1 di Tabel 3.3.**

> Petunjuk: "Setengah dari setengah tahun" bisa berarti stabil secara rata-rata, tetapi mungkin ada bulan-bulan dengan pertumbuhan signifikan yang tersembunyi di angka rata-rata.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.2 — Dari Tabel 3.2, apakah cross-filter bekerja untuk semua chart? Mengapa chart `Big Number with Trendline` tidak merespons cross-filter dari klik bar kategori, meskipun kedua chart ada di dashboard yang sama?**

> Petunjuk: Cross-filter bekerja ketika chart berbagi dimensi data yang sama (misalnya kolom `kategori`). Big Number mengaggregasi seluruh dataset tanpa dimensi — tidak ada dimensi yang bisa difilter.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.3 — Dari Tabel 3.3 (Query 2), kota dengan omzet tertinggi (dari Tabel 2.4 Latihan 2) belum tentu merupakan kota dengan `avg_per_transaksi` tertinggi. Apa yang bisa menyebabkan perbedaan ini? Dari perspektif bisnis, kota mana yang lebih "strategis" — kota dengan omzet total tinggi atau kota dengan nilai rata-rata per transaksi tinggi?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.4 — SQL Lab di Superset memungkinkan query ad-hoc langsung ke PostgreSQL. Bandingkan pendekatan ini dengan membuat chart baru di Explore: kapan SQL Lab lebih tepat digunakan, dan kapan Explore lebih tepat? Berikan satu skenario konkret untuk masing-masing.**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.5 — Fitur Native Filter di Superset berfungsi memfilter data di semua chart secara bersamaan. Dari perspektif desain dashboard, apa saja prinsip yang harus diperhatikan saat menentukan filter mana yang perlu ditambahkan ke sebuah dashboard bisnis? Berikan contoh filter yang berguna selain Time Range untuk studi kasus e-commerce ini.**

> Petunjuk: Pikirkan dimensi bisnis lain seperti `kategori`, `kota`, atau `segmen pelanggan` yang mungkin ingin difilter oleh pengguna dashboard.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 3

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Dashboard `Analitik Platform E-Commerce` berhasil dibuat dengan **___** chart yang tersusun dalam **___** baris layout. Native Filter `Periode Analisis` berhasil memfilter **___** dari **___** chart secara bersamaan. Dari SQL Lab, ditemukan bahwa bulan dengan MoM growth tertinggi adalah **___** (**___**%), dan kota dengan nilai rata-rata per transaksi tertinggi adalah **___** (Rp **___**). SQL Lab cocok digunakan untuk **___**, sedangkan Explore lebih tepat untuk **___**."

---

*Latihan 3 selesai. Lanjutkan ke **Latihan 4 — Dashboard Monitoring Infrastruktur Grafana**.*