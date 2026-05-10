# Latihan 2 — Supervised Learning: Regresi dan Klasifikasi
**Modul 9 · Machine Learning Big Data** | Estimasi waktu: **35 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Membangun pipeline Linear Regression untuk prediksi nilai transaksi kontinu
- Mengevaluasi model regresi menggunakan metrik RMSE, MAE, dan R²
- Membandingkan Logistic Regression dan Decision Tree pada tugas klasifikasi multi-kelas
- Membaca dan menginterpretasikan feature importance dari Decision Tree
- Mengamati perbedaan arsitektur pipeline di Spark UI

---

## Prasyarat

- [ ] Latihan 1 selesai — dataset tersedia di `hdfs:///datalake/silver/transaksi/`
- [ ] Kontainer berjalan dan semua layanan Hadoop aktif (`jps`)
- [ ] YARN UI dapat diakses di `http://localhost:8088`

---

## Bagian A — Linear Regression

### Langkah 2.1 — Buat skrip Linear Regression

Di dalam kontainer:

```bash
nano /tmp/linear_regression.py
```

Salin skrip berikut:

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, StandardScaler
)
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import time

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("LinearRegression-M9") \
        .master("yarn") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet("hdfs:///datalake/silver/transaksi/")

    # Encode kolom kategorikal
    cat_idx = StringIndexer(
        inputCol="kategori", outputCol="kat_idx",
        handleInvalid="keep"
    )
    ch_idx = StringIndexer(
        inputCol="channel", outputCol="ch_idx",
        handleInvalid="keep"
    )

    # Rakit semua fitur
    assembler = VectorAssembler(
        inputCols=["kuantitas", "harga_satuan",
                   "diskon", "berat_kg",
                   "kat_idx", "ch_idx"],
        outputCol="features_raw"
    )

    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features",
        withMean=True, withStd=True
    )

    lr = LinearRegression(
        featuresCol="features",
        labelCol="total_nilai",
        maxIter=100,
        regParam=0.1,
        elasticNetParam=0.0
    )

    pipeline = Pipeline(
        stages=[cat_idx, ch_idx, assembler, scaler, lr]
    )

    df_train, df_test = df.randomSplit([0.8, 0.2], seed=42)
    df_train.cache()

    t0 = time.time()
    model = pipeline.fit(df_train)
    durasi_train = round(time.time() - t0, 1)
    df_train.unpersist()

    df_pred = model.transform(df_test)

    evaluator = RegressionEvaluator(
        labelCol="total_nilai",
        predictionCol="prediction"
    )

    rmse = evaluator.setMetricName("rmse").evaluate(df_pred)
    mae  = evaluator.setMetricName("mae").evaluate(df_pred)
    r2   = evaluator.setMetricName("r2").evaluate(df_pred)

    print(f"\n{'='*50}")
    print(f" LINEAR REGRESSION — TEST SET")
    print(f"{'='*50}")
    print(f" Durasi training  : {durasi_train}s")
    print(f" RMSE             : {rmse:>15,.2f}")
    print(f" MAE              : {mae:>15,.2f}")
    print(f" R²               : {r2:>15.4f}")
    print(f"{'='*50}")

    # Koefisien model
    lr_model = model.stages[-1]
    feat_names = ["kuantitas", "harga_satuan", "diskon",
                  "berat_kg", "kat_idx", "ch_idx"]
    print("\n[Koefisien Model]")
    for fname, coef in zip(feat_names, lr_model.coefficients):
        print(f"  {fname:<15}: {coef:>15.2f}")
    print(f"  {'intercept':<15}: {lr_model.intercept:>15.2f}")

    # Tampilkan 10 prediksi vs aktual
    print("\n[Sampel Prediksi vs Aktual]")
    df_pred.select(
        F.round("total_nilai", 0).alias("aktual"),
        F.round("prediction", 0).alias("prediksi"),
        F.round(
            F.abs(F.col("total_nilai") - F.col("prediction")), 0
        ).alias("selisih_abs")
    ).show(10)

    spark.stop()
```

---

### Langkah 2.2 — Jalankan Linear Regression

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  --conf spark.sql.shuffle.partitions=20 \
  /tmp/linear_regression.py
```

Selama proses berjalan, buka `http://localhost:8088` dan amati job yang berjalan di tab **Applications**.

Catat semua metrik pada **Tabel 2.1**.

---

## Bagian B — Klasifikasi: Logistic Regression vs Decision Tree

### Langkah 2.3 — Buat skrip klasifikasi perbandingan

```bash
nano /tmp/klasifikasi_dt.py
```

Salin skrip berikut:

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, VectorAssembler, StandardScaler
)
from pyspark.ml.classification import (
    LogisticRegression, DecisionTreeClassifier
)
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator
)
import time

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Klasifikasi-M9") \
        .master("yarn") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(
        "hdfs:///datalake/silver/transaksi/"
    ).withColumn(
        "segmen",
        F.when(F.col("total_nilai") < 100_000, "rendah")
         .when(F.col("total_nilai") < 1_000_000, "menengah")
         .otherwise("tinggi")
    )

    # Preprocessing bersama
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
    assembler = VectorAssembler(
        inputCols=["kuantitas", "harga_satuan",
                   "diskon", "berat_kg",
                   "kat_idx", "ch_idx"],
        outputCol="features_raw"
    )
    scaler = StandardScaler(
        inputCol="features_raw", outputCol="features",
        withMean=True, withStd=True
    )

    df_train, df_test = df.randomSplit([0.8, 0.2], seed=42)
    df_train.cache()

    mc_eval = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction"
    )

    hasil = {}

    # ── Model 1: Logistic Regression ──────────────────────
    lr = LogisticRegression(
        featuresCol="features", labelCol="label",
        maxIter=100, regParam=0.01,
        family="multinomial"
    )
    pipe_lr = Pipeline(stages=[
        kat_idx, ch_idx, label_idx, assembler, scaler, lr
    ])

    t0 = time.time()
    model_lr = pipe_lr.fit(df_train)
    dur_lr = round(time.time() - t0, 1)

    pred_lr  = model_lr.transform(df_test)
    acc_lr   = mc_eval.setMetricName("accuracy").evaluate(pred_lr)
    f1_lr    = mc_eval.setMetricName("f1").evaluate(pred_lr)
    prec_lr  = mc_eval.setMetricName("weightedPrecision").evaluate(pred_lr)
    rec_lr   = mc_eval.setMetricName("weightedRecall").evaluate(pred_lr)
    hasil["Logistic Regression"] = {
        "accuracy": acc_lr, "f1": f1_lr,
        "precision": prec_lr, "recall": rec_lr,
        "durasi": dur_lr
    }

    # ── Model 2: Decision Tree ─────────────────────────────
    dt = DecisionTreeClassifier(
        featuresCol="features", labelCol="label",
        maxDepth=6, impurity="gini",
        minInstancesPerNode=10
    )
    pipe_dt = Pipeline(stages=[
        kat_idx, ch_idx, label_idx, assembler, scaler, dt
    ])

    t0 = time.time()
    model_dt = pipe_dt.fit(df_train)
    dur_dt = round(time.time() - t0, 1)

    pred_dt  = model_dt.transform(df_test)
    acc_dt   = mc_eval.setMetricName("accuracy").evaluate(pred_dt)
    f1_dt    = mc_eval.setMetricName("f1").evaluate(pred_dt)
    prec_dt  = mc_eval.setMetricName("weightedPrecision").evaluate(pred_dt)
    rec_dt   = mc_eval.setMetricName("weightedRecall").evaluate(pred_dt)
    hasil["Decision Tree"] = {
        "accuracy": acc_dt, "f1": f1_dt,
        "precision": prec_dt, "recall": rec_dt,
        "durasi": dur_dt
    }

    df_train.unpersist()

    # ── Ringkasan perbandingan ─────────────────────────────
    print(f"\n{'='*65}")
    print(f"{'Model':<25} {'Accuracy':>9} {'F1':>9} "
          f"{'Precision':>10} {'Recall':>9} {'Detik':>7}")
    print(f"{'='*65}")
    for nama, m in hasil.items():
        print(f"{nama:<25} {m['accuracy']:>9.4f} {m['f1']:>9.4f} "
              f"{m['precision']:>10.4f} {m['recall']:>9.4f} "
              f"{m['durasi']:>7.1f}s")
    print(f"{'='*65}")

    # ── Confusion matrix Decision Tree ────────────────────
    print("\n[Confusion Matrix — Decision Tree]")
    pred_dt.groupBy("segmen", "prediction") \
           .count() \
           .orderBy("segmen", "prediction") \
           .show()

    # ── Feature importance Decision Tree ──────────────────
    dt_model = model_dt.stages[-1]
    feat_names = ["kuantitas", "harga_satuan", "diskon",
                  "berat_kg", "kat_idx", "ch_idx"]
    print("\n[Feature Importance — Decision Tree]")
    importances = list(zip(feat_names, dt_model.featureImportances))
    for fname, imp in sorted(importances, key=lambda x: -x[1]):
        bar = "█" * int(imp * 50)
        print(f"  {fname:<15}: {imp:.4f}  {bar}")

    print(f"\nKedalaman pohon : {dt_model.depth}")
    print(f"Jumlah node     : {dt_model.numNodes}")

    spark.stop()
```

---

### Langkah 2.4 — Jalankan klasifikasi

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  --conf spark.sql.shuffle.partitions=20 \
  /tmp/klasifikasi_dt.py
```

Selama proses berjalan, buka `http://localhost:4040` (Spark UI) dan navigasi ke tab **SQL/DataFrame** untuk melihat query plan pipeline.

Catat semua output pada **Tabel 2.2**, **Tabel 2.3**, dan **Tabel 2.4**.

---

## Tabel Pencatatan Hasil

### Tabel 2.1 — Metrik Linear Regression

| Metrik | Nilai | Interpretasi |
|---|---|---|
| Durasi training (detik) | _..._ | _..._ |
| RMSE (test set) | Rp _..._ | _..._ |
| MAE (test set) | Rp _..._ | _..._ |
| R² (test set) | _..._ | _..._ (mendekati 1 = baik) |
| Koefisien terbesar (fitur) | _..._ | Fitur paling berpengaruh |
| Koefisien terkecil (fitur) | _..._ | Fitur paling tidak berpengaruh |

**Koefisien lengkap model:**

| Fitur | Koefisien | Arah Pengaruh (+ naik / – turun) |
|---|---|---|
| kuantitas | _..._ | _..._ |
| harga_satuan | _..._ | _..._ |
| diskon | _..._ | _..._ |
| berat_kg | _..._ | _..._ |
| kat_idx | _..._ | _..._ |
| ch_idx | _..._ | _..._ |
| intercept | _..._ | — |

**Sampel prediksi vs aktual (5 baris terburuk selisihnya):**

| Aktual (Rp) | Prediksi (Rp) | Selisih Abs (Rp) |
|---|---|---|
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |
| _..._ | _..._ | _..._ |

---

### Tabel 2.2 — Perbandingan Logistic Regression vs Decision Tree

| Metrik | Logistic Regression | Decision Tree | Model Lebih Baik |
|---|---|---|---|
| Accuracy | _..._ | _..._ | _..._ |
| F1-Score | _..._ | _..._ | _..._ |
| Weighted Precision | _..._ | _..._ | _..._ |
| Weighted Recall | _..._ | _..._ | _..._ |
| Durasi Training (detik) | _..._ | _..._ | _..._ (lebih cepat) |

### Tabel 2.3 — Confusion Matrix Decision Tree

*(isi dari output `groupBy("segmen","prediction").count().show()`)*

| Aktual ↓ / Prediksi → | rendah | menengah | tinggi |
|---|---|---|---|
| rendah | _..._ | _..._ | _..._ |
| menengah | _..._ | _..._ | _..._ |
| tinggi | _..._ | _..._ | _..._ |

**Dari confusion matrix:**

| Informasi | Nilai |
|---|---|
| Kelas yang paling sering salah diprediksi | _..._ |
| Kelas yang paling sering diklasifikasikan sebagai apa? | _..._ → _..._ |
| Jumlah prediksi benar total | _..._ |
| Jumlah prediksi salah total | _..._ |

### Tabel 2.4 — Feature Importance Decision Tree

| Rank | Fitur | Importance Score | Interpretasi |
|---|---|---|---|
| 1 | _..._ | _..._ | _..._ |
| 2 | _..._ | _..._ | _..._ |
| 3 | _..._ | _..._ | _..._ |
| 4 | _..._ | _..._ | _..._ |
| 5 | _..._ | _..._ | _..._ |
| 6 | _..._ | _..._ | _..._ |
| Kedalaman pohon | _..._ | — | — |
| Jumlah node | _..._ | — | — |

---

## Refleksi dan Analisis

**R2.1 — Dari Tabel 2.1, nilai R² Linear Regression. Jika R² mendekati 0, artinya model tidak mampu menjelaskan variansi `total_nilai`. Berdasarkan analisis korelasi di Latihan 1, mengapa Linear Regression mungkin kesulitan memodelkan `total_nilai` dengan baik? Transformasi apa yang bisa dicoba untuk meningkatkan R²?**

> Petunjuk: Ingat distribusi `total_nilai` yang sangat skewed. Apa yang terjadi jika kita menggunakan `log(total_nilai)` sebagai label?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.2 — Dari Tabel 2.1, koefisien `diskon` seharusnya bernilai negatif (diskon besar → total nilai berkurang). Apakah ini terjadi pada model Anda? Jika tidak, apa yang mungkin menyebabkan koefisien menunjukkan arah yang berlawanan dari ekspektasi?**

> Petunjuk: StandardScaler mengubah skala semua fitur. Koefisien yang terlihat adalah koefisien pada skala yang sudah dinormalisasi, bukan skala asli.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.3 — Dari Tabel 2.2, bandingkan Accuracy dan F1-Score kedua model. Accuracy tinggi tetapi F1 rendah adalah sinyal apa? Kaitkan dengan distribusi kelas yang tidak seimbang dari Latihan 1 (Tabel 1.5).**

> Petunjuk: Hitung manual: jika model selalu prediksi "tinggi" (kelas 77%), berapa accuracy-nya? Berapa F1-nya? Bandingkan dengan hasil model Anda.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.4 — Dari Tabel 2.3 (Confusion Matrix), kelas `rendah` hampir pasti paling banyak salah diklasifikasikan. Mengapa kelas dengan jumlah sampel terkecil (1.3%) paling susah diprediksi dengan benar? Strategi apa yang bisa digunakan untuk menangani masalah ini dalam produksi?**

> Petunjuk: Pikirkan tentang: class weights, oversampling (SMOTE), undersampling, atau mengubah threshold prediksi.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.5 — Dari Tabel 2.4, fitur mana yang paling penting menurut Decision Tree? Apakah hasilnya masuk akal secara bisnis? Mengapa `kat_idx` (kategori produk) dan `ch_idx` (channel) memiliki importance yang relatif rendah dibandingkan fitur numerik?**

> Petunjuk: Feature importance Decision Tree mengukur seberapa besar penurunan impurity (Gini) yang dihasilkan oleh split pada fitur tersebut. Fitur numerik dengan range lebar cenderung memiliki lebih banyak split point yang memungkinkan.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.6 — Dari Tabel 2.2, model mana yang lebih cepat dilatih? Secara umum, mengapa Decision Tree cenderung lebih cepat dari Logistic Regression untuk dataset kecil (~10K baris), tetapi bisa lebih lambat pada dataset besar (jutaan baris)?**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 2

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Linear Regression menghasilkan R² = **___**, yang menunjukkan model **___** (baik/buruk) dalam menjelaskan variansi `total_nilai`. Hal ini kemungkinan disebabkan oleh **___**. Untuk klasifikasi, **___** (Logistic Regression / Decision Tree) menghasilkan F1-Score lebih tinggi yaitu **___**. Fitur paling penting menurut Decision Tree adalah **___** dengan importance score **___**. Confusion matrix menunjukkan bahwa kelas yang paling sering salah diklasifikasikan adalah **___**, yang berkorelasi dengan fakta bahwa kelas ini hanya memiliki **___**% dari total data."

---

*Latihan 2 selesai. Lanjutkan ke **Latihan 3 — Unsupervised Learning: K-Means Clustering**.*
