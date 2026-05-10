# Latihan 3 — Unsupervised Learning: K-Means Clustering
**Modul 9 · Machine Learning Big Data** | Estimasi waktu: **20 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Melakukan agregasi fitur perilaku pelanggan dari data transaksi per baris
- Menerapkan StandardScaler sebelum K-Means untuk menghindari dominasi fitur berskala besar
- Menjalankan elbow method menggunakan silhouette score dan inertia untuk menentukan K optimal
- Menginterpretasikan profil setiap klaster secara bisnis
- Menyimpan hasil segmentasi ke Gold layer HDFS

---

## Prasyarat

- [ ] Latihan 1 dan 2 selesai
- [ ] Dataset tersedia di `hdfs:///datalake/silver/transaksi/`
- [ ] Direktori Gold sudah ada: `hdfs dfs -ls /datalake/gold/`

---

## Langkah Kerja

### Langkah 3.1 — Pahami transformasi data sebelum membuat skrip

K-Means pada modul ini **tidak** bekerja pada level transaksi (10.000 baris), melainkan pada level **pelanggan** (200 pelanggan unik). Data harus diagregasi terlebih dahulu menggunakan `groupBy("id_pelanggan")`.

Fitur yang dihasilkan dari agregasi:

| Fitur Agregat | Sumber | Makna Bisnis |
|---|---|---|
| `total_trx` | `count(*)` | Frekuensi belanja pelanggan |
| `total_belanja` | `sum(total_nilai)` | Total pengeluaran kumulatif |
| `avg_belanja` | `avg(total_nilai)` | Nilai rata-rata per transaksi |
| `maks_belanja` | `max(total_nilai)` | Transaksi terbesar yang pernah dilakukan |
| `ragam_kategori` | `countDistinct(kategori)` | Keragaman produk yang dibeli |

Dengan 5 fitur ini, K-Means akan mengelompokkan pelanggan ke dalam segmen perilaku.

---

### Langkah 3.2 — Eksplorasi data agregat via PySpark Shell (opsional, 5 menit)

Sebelum menjalankan K-Means, pahami distribusi fitur agregat:

```bash
pyspark --master yarn --executor-memory 512m --num-executors 2
```

```python
from pyspark.sql import functions as F

df = spark.read.parquet("hdfs:///datalake/silver/transaksi/")

# Agregasi per pelanggan
df_pel = df.groupBy("id_pelanggan").agg(
    F.count("*").alias("total_trx"),
    F.sum("total_nilai").alias("total_belanja"),
    F.avg("total_nilai").alias("avg_belanja"),
    F.max("total_nilai").alias("maks_belanja"),
    F.countDistinct("kategori").alias("ragam_kategori")
)

print(f"Jumlah pelanggan unik: {df_pel.count()}")
df_pel.describe().show()

# Lihat distribusi jumlah transaksi per pelanggan
df_pel.select("total_trx").summary("min","25%","50%","75%","max").show()
```

Catat statistik agregat pada **Tabel 3.1**. Ketik `exit()` untuk keluar.

---

### Langkah 3.3 — Buat skrip K-Means dengan elbow method

```bash
nano /tmp/kmeans_elbow.py
```

Salin skrip berikut:

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import time

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("KMeans-Elbow-M9") \
        .master("yarn") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df_raw = spark.read.parquet(
        "hdfs:///datalake/silver/transaksi/"
    )

    # ── Agregasi per pelanggan ──────────────────────────────
    df_pel = df_raw.groupBy("id_pelanggan").agg(
        F.count("*").alias("total_trx"),
        F.sum("total_nilai").alias("total_belanja"),
        F.avg("total_nilai").alias("avg_belanja"),
        F.max("total_nilai").alias("maks_belanja"),
        F.countDistinct("kategori").alias("ragam_kategori")
    )
    df_pel.cache()
    n_pelanggan = df_pel.count()
    print(f"\nJumlah pelanggan untuk clustering: {n_pelanggan}")

    # ── Preprocessing: rakit fitur + standarisasi ───────────
    assembler = VectorAssembler(
        inputCols=["total_trx", "total_belanja",
                   "avg_belanja", "maks_belanja",
                   "ragam_kategori"],
        outputCol="features_raw"
    )
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features",
        withMean=True, withStd=True
    )

    df_assembled = assembler.transform(df_pel)
    scaler_model = scaler.fit(df_assembled)
    df_feat = scaler_model.transform(df_assembled)
    df_feat.cache()

    # ── Elbow Method: K = 2 sampai 7 ───────────────────────
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        metricName="silhouette"
    )

    print(f"\n{'='*55}")
    print(f" ELBOW METHOD — Silhouette Score dan Inertia")
    print(f"{'='*55}")
    print(f"{'K':>3} | {'Silhouette':>12} | {'Inertia':>15} | {'Detik':>7}")
    print(f"{'-'*55}")

    hasil_elbow = []
    best_k, best_sil = 2, -1.0

    for k in range(2, 8):
        t0 = time.time()
        km = KMeans(
            featuresCol="features",
            k=k, maxIter=20, seed=42,
            initMode="k-means||"
        )
        km_model = km.fit(df_feat)
        df_pred  = km_model.transform(df_feat)

        sil     = evaluator.evaluate(df_pred)
        inertia = km_model.summary.trainingCost
        durasi  = round(time.time() - t0, 1)

        hasil_elbow.append({
            "k": k, "silhouette": sil,
            "inertia": inertia, "durasi": durasi
        })

        if sil > best_sil:
            best_sil = sil
            best_k   = k

        print(f"{k:>3} | {sil:>12.4f} | {inertia:>15.2f} | {durasi:>6.1f}s")

    print(f"{'='*55}")
    print(f"\n=> K optimal (silhouette tertinggi): K = {best_k}"
          f" (silhouette = {best_sil:.4f})")

    # ── Latih ulang dengan K optimal ───────────────────────
    print(f"\n[Melatih ulang dengan K={best_k}...]")
    km_final = KMeans(
        featuresCol="features",
        k=best_k, maxIter=20, seed=42,
        initMode="k-means||"
    )
    model_final   = km_final.fit(df_feat)
    df_clustered  = model_final.transform(df_feat)

    # ── Profil tiap klaster ────────────────────────────────
    print(f"\n[Profil Klaster (K={best_k})]")
    df_profile = df_clustered.join(
        df_pel, on="id_pelanggan"
    ).groupBy("prediction").agg(
        F.count("*").alias("n_pelanggan"),
        F.round(F.avg("total_trx"), 1).alias("avg_trx"),
        F.round(F.avg("total_belanja"), 0).alias("avg_total_belanja"),
        F.round(F.avg("avg_belanja"), 0).alias("avg_nilai_per_trx"),
        F.round(F.avg("maks_belanja"), 0).alias("avg_maks_belanja"),
        F.round(F.avg("ragam_kategori"), 2).alias("avg_ragam_kat")
    ).orderBy("prediction")

    df_profile.show(truncate=False)

    # ── Centroid tiap klaster ──────────────────────────────
    print("\n[Centroid Klaster (dalam skala standar)]")
    feat_names = ["total_trx","total_belanja","avg_belanja",
                  "maks_belanja","ragam_kategori"]
    for i, center in enumerate(model_final.clusterCenters()):
        print(f"\n  Klaster {i}:")
        for fname, val in zip(feat_names, center):
            print(f"    {fname:<20}: {val:>8.4f}")

    # ── Simpan ke Gold layer ───────────────────────────────
    df_clustered.select(
        "id_pelanggan",
        F.col("prediction").alias("klaster"),
        "total_trx", "total_belanja",
        "avg_belanja", "maks_belanja", "ragam_kategori"
    ).write.mode("overwrite").parquet(
        "hdfs:///datalake/gold/segmentasi_pelanggan/"
    )
    print("\n[OK] Hasil segmentasi disimpan ke Gold layer.")

    df_pel.unpersist()
    df_feat.unpersist()
    spark.stop()
```

---

### Langkah 3.4 — Jalankan K-Means

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  --conf spark.sql.shuffle.partitions=20 \
  /tmp/kmeans_elbow.py
```

Proses ini menjalankan **6 iterasi K-Means** (K=2 hingga K=7) secara berurutan, membutuhkan sekitar **5–10 menit**. Pantau di YARN UI `http://localhost:8088`.

Catat semua output pada **Tabel 3.2** dan **Tabel 3.3**.

---

### Langkah 3.5 — Verifikasi hasil tersimpan di HDFS

```bash
hdfs dfs -ls /datalake/gold/segmentasi_pelanggan/
hdfs dfs -du -h /datalake/gold/segmentasi_pelanggan/
```

Baca ulang hasil dari Gold layer untuk memastikan tersimpan dengan benar:

```bash
pyspark --master yarn --executor-memory 512m --num-executors 2
```

```python
df_gold = spark.read.parquet(
    "hdfs:///datalake/gold/segmentasi_pelanggan/"
)
print(f"Total baris: {df_gold.count()}")
df_gold.groupBy("klaster").count().orderBy("klaster").show()
df_gold.show(5)
exit()
```

Catat pada **Tabel 3.4**.

---

## Tabel Pencatatan Hasil

### Tabel 3.1 — Statistik Agregat per Pelanggan

*(dari eksplorasi Langkah 3.2 — `describe().show()`)*

| Statistik | total_trx | total_belanja | avg_belanja | maks_belanja | ragam_kategori |
|---|---|---|---|---|---|
| count | _..._ | _..._ | _..._ | _..._ | _..._ |
| mean | _..._ | _..._ | _..._ | _..._ | _..._ |
| stddev | _..._ | _..._ | _..._ | _..._ | _..._ |
| min | _..._ | _..._ | _..._ | _..._ | _..._ |
| max | _..._ | _..._ | _..._ | _..._ | _..._ |

**Range transaksi per pelanggan:**

| Persentil | total_trx |
|---|---|
| min (pelanggan paling jarang) | _..._ |
| 25% | _..._ |
| median (50%) | _..._ |
| 75% | _..._ |
| max (pelanggan paling sering) | _..._ |

### Tabel 3.2 — Hasil Elbow Method

*(isi dari output loop K=2 sampai K=7)*

| K | Silhouette Score | Inertia | Durasi (detik) | Keterangan |
|---|---|---|---|---|
| 2 | _..._ | _..._ | _..._ | _..._ |
| 3 | _..._ | _..._ | _..._ | _..._ |
| 4 | _..._ | _..._ | _..._ | _..._ |
| 5 | _..._ | _..._ | _..._ | _..._ |
| 6 | _..._ | _..._ | _..._ | _..._ |
| 7 | _..._ | _..._ | _..._ | _..._ |
| **K optimal** | **_..._** | **_..._** | — | **Silhouette tertinggi** |

**Trend yang diamati:**
- Silhouette tertinggi pada K = **___**
- Inertia menurun dari K=2 ke K=7? **Ya / Tidak**
- "Siku" (elbow) pada inertia terlihat di sekitar K = **___**

### Tabel 3.3 — Profil Klaster (K Optimal)

*(isi dari output `df_profile.show()`)*

| Klaster | N Pelanggan | Avg Trx | Avg Total Belanja | Avg Nilai/Trx | Avg Maks Belanja | Avg Ragam Kategori |
|---|---|---|---|---|---|---|
| 0 | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| 1 | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| 2 | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| _(tambah baris jika K > 3)_ | | | | | | |

**Interpretasi bisnis setiap klaster:**

| Klaster | Label Bisnis yang Diusulkan | Alasan |
|---|---|---|
| 0 | _contoh: "Pelanggan Premium"_ | _..._ |
| 1 | _contoh: "Pelanggan Reguler"_ | _..._ |
| 2 | _contoh: "Pelanggan Pasif"_ | _..._ |

### Tabel 3.4 — Verifikasi Gold Layer

| Informasi | Nilai |
|---|---|
| Total baris di Gold layer | _..._ |
| Jumlah pelanggan per klaster (klaster 0) | _..._ |
| Jumlah pelanggan per klaster (klaster 1) | _..._ |
| Jumlah pelanggan per klaster (klaster 2) | _..._ |
| Ukuran file di HDFS | _..._ |
| Apakah jumlah total = 200 pelanggan? | Ya / Tidak |

---

## Refleksi dan Analisis

**R3.1 — Dari Tabel 3.1, fitur `total_belanja` memiliki skala jutaan rupiah sedangkan `ragam_kategori` hanya bernilai 1–6. Tanpa StandardScaler, fitur mana yang akan mendominasi perhitungan jarak Euclidean K-Means? Apa dampaknya terhadap kualitas klaster yang terbentuk?**

> Petunjuk: Jarak Euclidean = √(Σ(xi - ci)²). Jika satu fitur bernilai 10.000.000 dan fitur lain bernilai 3, mana yang lebih berpengaruh terhadap total jarak?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.2 — Dari Tabel 3.2, apakah silhouette score selalu meningkat seiring bertambahnya K? Jelaskan mengapa silhouette score bisa menurun pada K tertentu meskipun inertia selalu menurun.**

> Petunjuk: Silhouette mengukur seberapa "terpisah" klaster satu dengan lainnya. Inertia hanya mengukur kekompakan internal. Apa yang terjadi saat K terlalu besar relatif terhadap ukuran dataset (200 pelanggan)?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.3 — Dari Tabel 3.3, deskripsikan secara bisnis perbedaan antara klaster yang memiliki `avg_nilai_per_trx` tinggi tetapi `avg_trx` rendah, dibandingkan klaster yang memiliki `avg_trx` tinggi tetapi `avg_nilai_per_trx` rendah. Strategi pemasaran apa yang tepat untuk masing-masing segmen?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.4 — K-Means menggunakan inisialisasi `k-means||` (varian K-Means++ untuk Spark). Mengapa inisialisasi centroid yang baik penting untuk konvergensi K-Means? Apa yang bisa terjadi jika centroid diinisialisasi secara acak biasa (random)?**

> Petunjuk: K-Means sensitif terhadap inisialisasi — centroid awal yang buruk bisa mengakibatkan konvergensi ke local optimum, bukan global optimum.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.5 — Dari Tabel 3.2, setiap nilai K dijalankan sebagai satu Spark job terpisah dalam loop Python. Jika dataset memiliki 10 juta pelanggan dan K-range = 2–10, estimasi berapa total Spark job yang dijalankan? Bagaimana cara yang lebih efisien untuk melakukan hyperparameter search pada skala besar?**

> Petunjuk: Lihat kembali Kode 100 di modul — `CrossValidator` dengan `ParamGridBuilder` dapat melakukan hal ini secara lebih terstruktur dengan evaluasi paralel.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.6 — Hasil segmentasi disimpan ke `hdfs:///datalake/gold/segmentasi_pelanggan/`. Dalam arsitektur medallion (Bronze–Silver–Gold), mengapa hasil ML seperti segmentasi pelanggan ini tepat diletakkan di Gold layer, bukan Silver?**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 3

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "K-Means dijalankan pada level **___** (transaksi / pelanggan) menggunakan **___** fitur agregat. Elbow method menentukan K optimal = **___** dengan silhouette score = **___**. Klaster dengan avg_nilai_per_trx tertinggi adalah klaster **___** yang terdiri dari **___** pelanggan — dapat dikategorikan sebagai **___**. StandardScaler diperlukan sebelum K-Means karena **___**. Hasil segmentasi disimpan ke Gold layer karena **___**."

---

*Latihan 3 selesai. Lanjutkan ke **Latihan 4 — Pipeline ML End-to-End**.*
