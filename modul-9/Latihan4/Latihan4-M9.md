# Latihan 4 — Pipeline ML End-to-End
**Modul 9 · Machine Learning Big Data** | Estimasi waktu: **15 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Membangun pipeline ML end-to-end yang mencakup preprocessing, training, evaluasi, dan penyimpanan model
- Menyimpan `PipelineModel` ke HDFS sebagai model registry sederhana
- Memuat kembali model dari HDFS dan menggunakannya untuk batch inference
- Mengamati perilaku Spark job selama proses `pipeline.fit()` melalui Spark UI
- Memverifikasi hasil prediksi yang tersimpan di Gold layer

---

## Prasyarat

- [ ] Latihan 1, 2, dan 3 selesai
- [ ] Dataset tersedia di `hdfs:///datalake/silver/transaksi/`
- [ ] Direktori model sudah ada: `hdfs dfs -ls /models/`
- [ ] Spark UI dapat diakses di `http://localhost:4040` saat job berjalan

---

## Langkah Kerja

### Langkah 4.1 — Pahami perbedaan Pipeline E2E dengan skrip individual

Pada Latihan 2, setiap model dibuat dan dievaluasi secara terpisah. Pipeline E2E di latihan ini menggabungkan seluruh tahap menjadi satu alur yang:

1. **Memuat data** dari Silver layer
2. **Menyiapkan label** dengan `withColumn`
3. **Membersihkan null** dengan `Imputer`
4. **Mengkodekan** fitur kategorikal dengan `StringIndexer`
5. **Merakit** semua fitur dengan `VectorAssembler`
6. **Menormalisasi** dengan `StandardScaler`
7. **Melatih model** `DecisionTreeClassifier`
8. **Mengevaluasi** pada test set
9. **Menyimpan model** ke HDFS
10. **Menyimpan prediksi** ke Gold layer

Pendekatan ini menghilangkan risiko *training-serving skew* — preprocessing yang berbeda antara saat training dan saat inference.

---

### Langkah 4.2 — Buat skrip pipeline end-to-end

```bash
nano /tmp/pipeline_ml_e2e.py
```

Salin skrip berikut:

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    StringIndexer, VectorAssembler,
    StandardScaler, Imputer
)
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import time

SILVER_TRX  = "hdfs:///datalake/silver/transaksi/"
GOLD_PRED   = "hdfs:///datalake/gold/prediksi_segmen/"
MODEL_PATH  = "hdfs:///models/segmentasi_dt/v1"

def buat_spark():
    return SparkSession.builder \
        .appName("ML-Pipeline-E2E-M9") \
        .master("yarn") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()

def muat_dan_siapkan_data(spark):
    """Baca Silver, buat label segmen, buang null kritis."""
    df = spark.read.parquet(SILVER_TRX)
    df = df.withColumn(
        "segmen",
        F.when(F.col("total_nilai") < 100_000, "rendah")
         .when(F.col("total_nilai") < 1_000_000, "menengah")
         .otherwise("tinggi")
    )
    return df.dropna(subset=["total_nilai", "kuantitas", "harga_satuan"])

def bangun_pipeline():
    """Bangun Pipeline Spark ML lengkap."""
    # Isi null kolom diskon dengan median
    imputer = Imputer(
        inputCols=["diskon"],
        outputCols=["diskon_imp"],
        strategy="median"
    )
    # Encode kolom kategorikal
    kat_idx = StringIndexer(
        inputCol="kategori", outputCol="kat_idx",
        handleInvalid="keep"
    )
    ch_idx = StringIndexer(
        inputCol="channel", outputCol="ch_idx",
        handleInvalid="keep"
    )
    label_idx = StringIndexer(
        inputCol="segmen", outputCol="label",
        handleInvalid="keep"
    )
    # Rakit fitur
    assembler = VectorAssembler(
        inputCols=["kuantitas", "harga_satuan",
                   "diskon_imp", "berat_kg",
                   "kat_idx", "ch_idx"],
        outputCol="features_raw"
    )
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features",
        withMean=True, withStd=True
    )
    # Model
    dt = DecisionTreeClassifier(
        featuresCol="features",
        labelCol="label",
        maxDepth=6,
        minInstancesPerNode=5,
        impurity="gini"
    )
    return Pipeline(stages=[
        imputer, kat_idx, ch_idx, label_idx,
        assembler, scaler, dt
    ])

def evaluasi_dan_cetak(df_pred, nama_split):
    """Evaluasi dan cetak metrik klasifikasi."""
    mc = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction"
    )
    metrics = {
        "accuracy":  mc.setMetricName("accuracy").evaluate(df_pred),
        "f1":        mc.setMetricName("f1").evaluate(df_pred),
        "precision": mc.setMetricName("weightedPrecision").evaluate(df_pred),
        "recall":    mc.setMetricName("weightedRecall").evaluate(df_pred),
    }
    print(f"\n  [{nama_split}]")
    for nama, nilai in metrics.items():
        print(f"    {nama:<12}: {nilai:.4f}")
    return metrics

if __name__ == "__main__":
    spark = buat_spark()
    spark.sparkContext.setLogLevel("WARN")
    t_total = time.time()

    # ── [1/5] Muat dan siapkan data ──────────────────────
    print("\n" + "="*55)
    print(" [1/5] MEMUAT DATA")
    print("="*55)
    df = muat_dan_siapkan_data(spark)
    n_total = df.count()
    print(f"  Total baris setelah filter null : {n_total:,}")
    df_train, df_test = df.randomSplit([0.8, 0.2], seed=42)
    n_train = df_train.count()
    n_test  = df_test.count()
    print(f"  Train : {n_train:,} baris")
    print(f"  Test  : {n_test:,} baris")

    # ── [2/5] Latih pipeline ─────────────────────────────
    print("\n" + "="*55)
    print(" [2/5] MELATIH PIPELINE")
    print("="*55)
    df_train.cache()
    pipeline = bangun_pipeline()

    t0 = time.time()
    model = pipeline.fit(df_train)
    durasi_train = round(time.time() - t0, 1)
    df_train.unpersist()
    print(f"  Durasi training : {durasi_train}s")

    # ── [3/5] Evaluasi ───────────────────────────────────
    print("\n" + "="*55)
    print(" [3/5] EVALUASI MODEL")
    print("="*55)
    df_pred_train = model.transform(df_train)
    df_pred_test  = model.transform(df_test)
    m_train = evaluasi_dan_cetak(df_pred_train, "Training Set")
    m_test  = evaluasi_dan_cetak(df_pred_test,  "Test Set")

    # Selisih accuracy training vs test (deteksi overfitting)
    gap = m_train["accuracy"] - m_test["accuracy"]
    print(f"\n  Gap accuracy (train-test) : {gap:.4f}")
    if gap > 0.05:
        print("  ⚠ Potensi overfitting terdeteksi (gap > 0.05)")
    else:
        print("  ✓ Generalisasi baik (gap ≤ 0.05)")

    # ── [4/5] Simpan model ───────────────────────────────
    print("\n" + "="*55)
    print(" [4/5] MENYIMPAN MODEL KE HDFS")
    print("="*55)
    model.write().overwrite().save(MODEL_PATH)
    print(f"  Model disimpan  : {MODEL_PATH}")

    # ── [5/5] Simpan prediksi ke Gold ───────────────────
    print("\n" + "="*55)
    print(" [5/5] MENYIMPAN PREDIKSI KE GOLD LAYER")
    print("="*55)
    df_pred_test.select(
        "id_transaksi", "segmen",
        F.col("prediction").cast("integer").alias("pred_idx"),
        F.current_timestamp().alias("inference_time")
    ).write.mode("overwrite").parquet(GOLD_PRED)
    print(f"  Prediksi disimpan : {GOLD_PRED}")

    durasi_total = round(time.time() - t_total, 1)
    print(f"\n{'='*55}")
    print(f" Pipeline selesai dalam {durasi_total}s total")
    print(f"{'='*55}")

    spark.stop()
```

---

### Langkah 4.3 — Jalankan pipeline dan amati Spark UI

Buka dua terminal secara bersamaan. Di terminal pertama, jalankan pipeline:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  --conf spark.sql.shuffle.partitions=20 \
  /tmp/pipeline_ml_e2e.py
```

Di terminal kedua (atau browser), buka `http://localhost:4040` saat job berjalan dan navigasi ke:
- **Jobs** — amati berapa job yang terbentuk selama `pipeline.fit()`
- **Stages** — identifikasi stage paling berat (shuffle terbesar)
- **SQL/DataFrame** — lihat query plan, cari operator VectorAssembler dan StandardScaler

Catat pengamatan pada **Tabel 4.2**.

---

### Langkah 4.4 — Verifikasi model tersimpan di HDFS

```bash
# Cek struktur direktori model
hdfs dfs -ls /models/segmentasi_dt/v1/

# Cek ukuran model
hdfs dfs -du -h /models/segmentasi_dt/v1/

# Cek isi prediksi di Gold layer
hdfs dfs -ls /datalake/gold/prediksi_segmen/
hdfs dfs -du -h /datalake/gold/prediksi_segmen/
```

Catat pada **Tabel 4.3**.

---

### Langkah 4.5 — Muat model dan lakukan batch inference

Buat skrip inference terpisah untuk mensimulasikan deployment:

```bash
nano /tmp/inference.py
```

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import PipelineModel

spark = SparkSession.builder \
    .appName("Inference-M9") \
    .master("yarn") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── Muat model dari HDFS ──────────────────────────────────
print("[1/3] Memuat model dari HDFS...")
model = PipelineModel.load("hdfs:///models/segmentasi_dt/v1")
print(f"      Jumlah stage dalam pipeline : {len(model.stages)}")
for i, stage in enumerate(model.stages):
    print(f"      Stage {i}: {type(stage).__name__}")

# ── Baca data baru (simulasi: 200 baris dari Silver) ─────
print("\n[2/3] Membaca data baru untuk inference...")
df_baru = spark.read.parquet(
    "hdfs:///datalake/silver/transaksi/"
).limit(200)
print(f"      Baris data baru : {df_baru.count()}")

# ── Jalankan inference ────────────────────────────────────
# Tambah kolom segmen (diperlukan pipeline karena ada StringIndexer label)
df_baru = df_baru.withColumn(
    "segmen",
    F.when(F.col("total_nilai") < 100_000, "rendah")
     .when(F.col("total_nilai") < 1_000_000, "menengah")
     .otherwise("tinggi")
)

print("\n[3/3] Menjalankan prediksi...")
df_hasil = model.transform(df_baru)

print(f"\n      Total diprediksi : {df_hasil.count()}")
print("\n[Distribusi prediksi]")
df_hasil.groupBy(
    F.col("prediction").cast("integer").alias("pred_idx")
).count().orderBy("pred_idx").show()

print("\n[Sampel hasil prediksi]")
df_hasil.select(
    "id_transaksi", "kategori",
    F.round("total_nilai", 0).alias("total_nilai"),
    "segmen",
    F.col("prediction").cast("integer").alias("prediksi")
).show(10)

spark.stop()
```

Jalankan:

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  /tmp/inference.py
```

Catat output pada **Tabel 4.4**.

---

## Tabel Pencatatan Hasil

### Tabel 4.1 — Ringkasan Eksekusi Pipeline

| Tahap | Keterangan | Durasi / Nilai |
|---|---|---|
| Total baris setelah filter null | _..._ | _..._ baris |
| Baris training (80%) | _..._ | _..._ baris |
| Baris test (20%) | _..._ | _..._ baris |
| Durasi `pipeline.fit()` | _..._ | _..._ detik |
| Durasi total pipeline | _..._ | _..._ detik |
| Accuracy — Training Set | _..._ | _..._ |
| F1-Score — Training Set | _..._ | _..._ |
| Accuracy — Test Set | _..._ | _..._ |
| F1-Score — Test Set | _..._ | _..._ |
| Gap accuracy (train - test) | _..._ | _..._ |
| Status overfitting | _..._ | Normal / ⚠ Overfitting |

### Tabel 4.2 — Pengamatan Spark UI Selama `pipeline.fit()`

| Pengamatan | Nilai | Keterangan |
|---|---|---|
| Total job yang terbentuk | _..._ | Amati di tab Jobs |
| Job dengan durasi terlama | Job #_..._ | _..._ detik |
| Stage dengan shuffle read terbesar | Stage #_..._ | _..._ MB |
| Operator yang terlihat di SQL plan | _..._ | VectorAssembler, Scaler, dll. |
| Memory yang digunakan (Storage tab) | _..._ | MB/KB |
| Apakah `.cache()` terlihat di Storage? | Ya / Tidak | _..._ |

### Tabel 4.3 — Verifikasi Artefak di HDFS

| Artefak | Path HDFS | Ukuran | Ada? |
|---|---|---|---|
| Model PipelineModel | `/models/segmentasi_dt/v1/` | _..._ | Ya / Tidak |
| Prediksi Gold layer | `/datalake/gold/prediksi_segmen/` | _..._ | Ya / Tidak |
| Segmentasi pelanggan (dari Lab 3) | `/datalake/gold/segmentasi_pelanggan/` | _..._ | Ya / Tidak |

**Struktur direktori model (`hdfs dfs -ls /models/segmentasi_dt/v1/`):**

```
(salin output di sini)
```

### Tabel 4.4 — Hasil Batch Inference

| Informasi | Nilai |
|---|---|
| Total baris yang diprediksi | _..._ |
| Distribusi prediksi — klaster 0 | _..._ baris |
| Distribusi prediksi — klaster 1 | _..._ baris |
| Distribusi prediksi — klaster 2 | _..._ baris |
| Jumlah stage pipeline saat inference | _..._ |
| Apakah stage StringIndexer untuk label dieksekusi? | Ya / Tidak |

**Perbandingan label aktual vs prediksi (10 baris sampel):**

| id_transaksi | kategori | total_nilai | Segmen Aktual | Prediksi |
|---|---|---|---|---|
| _..._ | _..._ | _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ | _..._ | _..._ |

---

## Refleksi dan Analisis

**R4.1 — Dari Tabel 4.1, bandingkan accuracy Training Set dan Test Set. Jika gap-nya kecil (≤ 0.05), apakah itu berarti model sudah sempurna? Apa yang mungkin tidak terdeteksi oleh gap accuracy saja, terutama pada dataset dengan kelas tidak seimbang?**

> Petunjuk: Bayangkan model yang selalu memprediksi kelas mayoritas — gap accuracy-nya kecil, tetapi F1 kelas minoritas akan = 0.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.2 — Dari Tabel 4.2, berapa job yang terbentuk selama `pipeline.fit()`? Setiap `Estimator` dalam pipeline (Imputer, StringIndexer, StandardScaler, DecisionTree) biasanya membutuhkan setidaknya satu Spark action. Jelaskan mengapa setiap Estimator perlu "melihat" seluruh data sebelum bisa men-transform data tersebut.**

> Petunjuk: `StandardScaler.fit()` perlu menghitung mean dan stddev seluruh kolom. `StringIndexer.fit()` perlu mengetahui semua nilai unik yang ada. Ini berbeda dari `Transformer` yang tidak membutuhkan fit.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.3 — Pipeline menyimpan seluruh tahap preprocessing beserta model ke HDFS sebagai satu unit (`PipelineModel`). Apa keuntungan konkret dari pendekatan ini dibandingkan menyimpan model Decision Tree saja (tanpa preprocessing)?**

> Petunjuk: Pikirkan skenario: data inference baru datang dalam format raw (string kategori, belum di-encode). Siapa yang akan mengerjakan encoding jika preprocessing tidak disimpan bersama model?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.4 — Dari Tabel 4.4, saat inference dijalankan, semua stage pipeline dieksekusi termasuk `StringIndexer` untuk kolom `segmen` (label). Dalam deployment produksi nyata, label aktual mungkin tidak tersedia saat inference (karena itulah kita butuh prediksi). Bagaimana cara yang benar mendesain pipeline agar bisa dipakai untuk inference tanpa label?**

> Petunjuk: Pertimbangkan memisahkan pipeline preprocessing+model dari pipeline yang menyertakan label encoder. Atau gunakan `handleInvalid="keep"` pada StringIndexer label.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.5 — Dari Tabel 4.3, model `PipelineModel` tersimpan sebagai folder di HDFS (bukan satu file tunggal). Di dalam folder tersebut terdapat subfolder untuk setiap stage. Apa keuntungan dan kerugian menyimpan model di HDFS dibandingkan menggunakan tools seperti MLflow untuk model registry?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.6 — Pipeline E2E ini menjalankan semua tahap secara sequential dalam satu skrip. Dalam sistem produksi nyata, tahap mana yang idealnya dipisahkan menjadi job terpisah dan dijadwalkan secara berkala? Berikan alasannya.**

> Contoh: Apakah data preparation, training, evaluasi, dan inference harus selalu berjalan bersamaan?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 4

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Pipeline ML end-to-end terdiri dari **___** stage, di mana **___** stage adalah Estimator (membutuhkan `.fit()`) dan **___** stage adalah Transformer. Durasi `pipeline.fit()` adalah **___** detik dengan membentuk total **___** Spark job. Accuracy test set = **___** dengan gap terhadap training = **___** — menunjukkan model **___** (overfit / generalisasi baik). Model disimpan ke HDFS di path **___** sebagai satu `PipelineModel` yang mencakup seluruh tahap preprocessing, sehingga saat inference data baru **___** (perlu / tidak perlu) di-preprocessing ulang secara manual."

---

*Latihan 4 selesai. Lanjutkan ke **Latihan 5 — Eksplorasi: Regularisasi, Kedalaman Pohon, dan Diskusi**.*
