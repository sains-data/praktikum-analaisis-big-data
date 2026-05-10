# Latihan 1 — Persiapan Data dan Eksplorasi Awal
**Modul 9 · Machine Learning Big Data** | Estimasi waktu: **10 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Memverifikasi lingkungan Hadoop + Spark berjalan di dalam kontainer Docker
- Mengunggah dataset transaksi ke HDFS sebagai Bronze dan Silver layer
- Mengeksplorasi dataset secara statistik menggunakan PySpark Shell
- Mengidentifikasi distribusi kelas, nilai null, dan statistik deskriptif sebagai fondasi pemodelan

---

## Prasyarat

- [ ] Kontainer `bigdata-spark` sudah berjalan (`docker ps`)
- [ ] Bootstrap selesai (`docker exec bigdata-spark cat /tmp/bootstrap.log | tail -3`)
- [ ] File `transaksi_ml.json` tersedia di folder `modul9/data/`
- [ ] Library `pyspark` terpasang di dalam kontainer

---

## Langkah Kerja

### Langkah 1.1 — Masuk ke kontainer dan verifikasi layanan

Dari terminal WSL Ubuntu, jalankan:

```bash
bash start.sh
bash login.sh
```

Di dalam kontainer, cek proses Java yang aktif:

```bash
jps
```

Pastikan **keempat proses** ini muncul:

```
NameNode
DataNode
ResourceManager
NodeManager
```

Jika ada yang tidak muncul, jalankan:

```bash
start-dfs.sh && start-yarn.sh
```

Tunggu 15 detik lalu jalankan `jps` kembali.

---

### Langkah 1.2 — Buat dan jalankan script generator dataset

Salin script generator ke `/tmp/`:

```bash
cp /modul9/scripts/buat_data_ml.py /tmp/buat_data_ml.py
```

Jika direktori `/modul9/` belum ter-mount, buat script langsung:

```bash
nano /tmp/buat_data_ml.py
# (salin isi script dari bagian Konfigurasi Lab modul9-lab-setup.md)
```

Jalankan dengan `spark-submit`:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  /tmp/buat_data_ml.py
```

Proses ini membutuhkan **2–5 menit**. Output akhir yang diharapkan:

```
[OK] Dataset dibuat: 10000 baris
root
 |-- id_transaksi: string (nullable = true)
 |-- id_pelanggan: string (nullable = true)
 ...
```

---

### Langkah 1.3 — Verifikasi data tersedia di HDFS

```bash
# Cek file tersimpan
hdfs dfs -ls /datalake/silver/transaksi/

# Cek ukuran
hdfs dfs -du -h /datalake/silver/transaksi/

# Cek Bronze layer
hdfs dfs -ls /datalake/bronze/transaksi/
```

Catat jumlah file partisi dan ukuran data pada **Tabel 1.1**.

---

### Langkah 1.4 — Eksplorasi awal via PySpark Shell

Buka PySpark Shell interaktif:

```bash
pyspark \
  --master yarn \
  --executor-memory 512m \
  --num-executors 2
```

Setelah prompt `>>>` muncul, jalankan kode berikut **satu blok sekaligus**:

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, when, isnan

df = spark.read.parquet("hdfs:///datalake/silver/transaksi/")

# --- Dimensi dataset ---
print(f"Total baris : {df.count()}")
print(f"Total kolom : {len(df.columns)}")
print(f"\nSkema:")
df.printSchema()
```

Catat output pada **Tabel 1.1**.

```python
# --- Statistik deskriptif kolom numerik ---
print("\nStatistik deskriptif:")
df.select("kuantitas", "harga_satuan", "diskon", "total_nilai").describe().show()
```

Catat semua nilai pada **Tabel 1.2**.

```python
# --- Distribusi kategori ---
print("\nDistribusi kategori:")
df.groupBy("kategori").count() \
  .orderBy(F.col("count").desc()).show()

# --- Distribusi channel ---
print("\nDistribusi channel:")
df.groupBy("channel").count() \
  .orderBy(F.col("count").desc()).show()
```

Catat pada **Tabel 1.3**.

```python
# --- Cek nilai null di setiap kolom ---
print("\nJumlah nilai null per kolom:")
df.select([
    count(when(col(c).isNull() | isnan(col(c)), c)).alias(c)
    for c in df.columns
]).show()
```

Catat pada **Tabel 1.4**.

```python
# --- Distribusi label yang akan digunakan ---
# Derive segmen seperti yang akan dilakukan di skrip ML
df_label = df.withColumn(
    "segmen",
    F.when(F.col("total_nilai") < 100_000, "rendah")
     .when(F.col("total_nilai") < 1_000_000, "menengah")
     .otherwise("tinggi")
)

print("\nDistribusi segmen (label klasifikasi):")
df_label.groupBy("segmen").count() \
        .orderBy(F.col("count").desc()).show()

print("\nDistribusi label biner (total_nilai > 1 juta):")
df.withColumn("label_biner",
    (F.col("total_nilai") > 1_000_000).cast("integer")
).groupBy("label_biner").count().show()
```

Catat pada **Tabel 1.5**.

```python
# --- Korelasi antar fitur numerik ---
print("\nKorelasi total_nilai dengan fitur numerik:")
for col_name in ["kuantitas", "harga_satuan", "diskon", "berat_kg"]:
    corr = df.stat.corr("total_nilai", col_name)
    print(f"  corr(total_nilai, {col_name}) = {corr:.4f}")
```

Catat pada **Tabel 1.6**. Ketik `exit()` untuk keluar dari PySpark Shell.

---

### Langkah 1.5 — Verifikasi melalui HDFS Web UI

Buka browser ke `http://localhost:9870`, navigasi ke:

**Utilities → Browse the file system → /datalake/silver/transaksi/**

Catat informasi yang terlihat pada **Tabel 1.1**.

---

## Tabel Pencatatan Hasil

### Tabel 1.1 — Verifikasi Dataset di HDFS

| Informasi | Nilai yang Tercatat |
|---|---|
| Jumlah file partisi di `/datalake/silver/transaksi/` | _..._ |
| Ukuran total data Silver (KB/MB) | _..._ |
| Ukuran total data Bronze (KB/MB) | _..._ |
| Apakah Bronze dan Silver berukuran sama? | Ya / Tidak |
| Total baris dataset (`df.count()`) | _..._ |
| Total kolom | _..._ |

### Tabel 1.2 — Statistik Deskriptif Kolom Numerik

*(salin dari output `describe().show()`)*

| Statistik | kuantitas | harga_satuan | diskon | total_nilai |
|---|---|---|---|---|
| count | _..._ | _..._ | _..._ | _..._ |
| mean | _..._ | _..._ | _..._ | _..._ |
| stddev | _..._ | _..._ | _..._ | _..._ |
| min | _..._ | _..._ | _..._ | _..._ |
| max | _..._ | _..._ | _..._ | _..._ |

### Tabel 1.3 — Distribusi Kategori dan Channel

**Distribusi Kategori:**

| Kategori | Jumlah | Persentase (%) |
|---|---|---|
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |
| **Total** | **10.000** | **100%** |

**Distribusi Channel:**

| Channel | Jumlah | Persentase (%) |
|---|---|---|
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |

### Tabel 1.4 — Pemeriksaan Nilai Null

| Kolom | Jumlah Null |
|---|---|
| id_transaksi | _..._ |
| id_pelanggan | _..._ |
| kategori | _..._ |
| channel | _..._ |
| kuantitas | _..._ |
| harga_satuan | _..._ |
| diskon | _..._ |
| total_nilai | _..._ |
| berat_kg | _..._ |
| **Total null keseluruhan** | _..._ |

### Tabel 1.5 — Distribusi Label

**Segmen (label multi-kelas untuk klasifikasi):**

| Segmen | Jumlah | Persentase (%) |
|---|---|---|
| tinggi | _..._ | _..._ |
| menengah | _..._ | _..._ |
| rendah | _..._ | _..._ |

**Label Biner (untuk Logistic Regression biner):**

| Label Biner | Makna | Jumlah | Persentase (%) |
|---|---|---|---|
| 1 | total_nilai > Rp 1 juta | _..._ | _..._ |
| 0 | total_nilai ≤ Rp 1 juta | _..._ | _..._ |

### Tabel 1.6 — Korelasi dengan `total_nilai`

| Fitur | Nilai Korelasi | Interpretasi (kuat/sedang/lemah, positif/negatif) |
|---|---|---|
| kuantitas | _..._ | _..._ |
| harga_satuan | _..._ | _..._ |
| diskon | _..._ | _..._ |
| berat_kg | _..._ | _..._ |
| **Fitur paling berkorelasi** | _..._ | _..._ |

---

## Refleksi dan Analisis

**R1.1 — Dari Tabel 1.2, nilai `stddev` pada `total_nilai` sangat besar dibandingkan `mean`-nya. Apa artinya ini secara statistik? Apa dampaknya terhadap algoritma Linear Regression yang akan dilatih di Tahap 2?**

> Petunjuk: Bayangkan jika Anda memplot distribusi `total_nilai` — apakah akan berbentuk normal (Gaussian) atau skewed? Mengapa hal ini penting sebelum melatih model regresi?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.2 — Dari Tabel 1.5, distribusi segmen sangat tidak seimbang: kelas `tinggi` mendominasi sekitar 77%, sementara `rendah` hanya sekitar 1%. Jika sebuah model selalu memprediksi "tinggi" untuk semua transaksi, berapa accuracy-nya? Apakah model seperti itu berguna?**

> Petunjuk: Ini adalah konsep "dummy classifier". Hitung: berapa accuracy jika semua prediksi = "tinggi"?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.3 — Dari Tabel 1.6, fitur mana yang paling berkorelasi dengan `total_nilai`? Apakah hasil ini masuk akal secara bisnis? Apakah korelasi tinggi antara fitur dan label selalu berarti fitur tersebut adalah prediktor yang baik dalam model ML?**

> Petunjuk: Ingat perbedaan antara korelasi linear (yang diukur `stat.corr`) dan hubungan non-linear.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.4 — Dari Tabel 1.4, apakah ada nilai null di dataset? Dalam konteks Spark MLlib, apa yang terjadi pada `VectorAssembler` jika menemukan baris dengan nilai null di kolom fitur? Bagaimana cara mengatasinya?**

> Petunjuk: Lihat parameter `handleInvalid` di `VectorAssembler` — nilai apa yang tersedia dan apa perbedaannya?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.5 — Dari Tabel 1.3, apakah distribusi kategori merata? Mengapa data yang di-generate menggunakan `random.seed(42)` menghasilkan distribusi yang hampir merata antar kategori, tetapi distribusi `total_nilai` sangat skewed ke kelas `tinggi`?**

> Petunjuk: Perhatikan nilai `base_harga` per kategori di script generator — `otomotif` = 800.000, `makanan` = 50.000. Lalu `kuantitas` bisa mencapai 20. Hitung nilai transaksi otomotif maksimum vs minimum transaksi makanan.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 1

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Dataset berhasil dimuat ke HDFS dengan total **___** baris dan **___** kolom numerik serta **___** kolom kategorikal. Nilai null ditemukan sebanyak **___**. Label klasifikasi `segmen` memiliki distribusi yang **___** (seimbang/tidak seimbang), di mana kelas dominan adalah **___** dengan **___**%. Fitur yang paling berkorelasi dengan `total_nilai` adalah **___** dengan korelasi **___**. Hal ini menunjukkan bahwa **___** merupakan prediktor utama nilai transaksi dalam dataset ini."

---

*Latihan 1 selesai. Lanjutkan ke **Latihan 2 — Supervised Learning: Regresi dan Klasifikasi**.*
