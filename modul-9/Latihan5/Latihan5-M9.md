# Latihan 5 — Eksplorasi: Regularisasi, Kedalaman Pohon, dan Diskusi
**Modul 9 · Machine Learning Big Data** | Estimasi waktu: **10 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Mengamati pengaruh nilai `regParam` terhadap performa Linear Regression
- Mengidentifikasi titik overfitting pada Decision Tree berdasarkan variasi `maxDepth`
- Menjelaskan konsep bias-variance tradeoff melalui hasil eksperimen
- Menjawab pertanyaan diskusi konseptual yang menghubungkan implementasi dengan teori ML
- Merangkum seluruh pengalaman praktikum Modul 9 dalam satu narasi terpadu

---

## Prasyarat

- [ ] Latihan 1–4 sudah selesai
- [ ] Dataset tersedia di `hdfs:///datalake/silver/transaksi/`
- [ ] Kontainer masih berjalan

---

## Bagian A — Pengaruh Regularisasi pada Linear Regression

### Langkah A.1 — Buat skrip eksperimen regParam

```bash
nano /tmp/eksplorasi_regparam.py
```

Salin skrip berikut:

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler, StringIndexer
)
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("RegParam-Eksplorasi-M9") \
        .master("yarn") \
        .config("spark.sql.shuffle.partitions", "20") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet("hdfs:///datalake/silver/transaksi/")

    kat_idx = StringIndexer(
        inputCol="kategori", outputCol="kat_idx",
        handleInvalid="keep"
    )
    ch_idx = StringIndexer(
        inputCol="channel", outputCol="ch_idx",
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

    evaluator = RegressionEvaluator(
        labelCol="total_nilai",
        predictionCol="prediction"
    )

    reg_values = [0.0, 0.001, 0.01, 0.1, 1.0, 10.0]

    print(f"\n{'='*60}")
    print(f" EKSPERIMEN REGULARISASI LINEAR REGRESSION")
    print(f"{'='*60}")
    print(f"{'regParam':>10} | {'RMSE Train':>14} | {'RMSE Test':>14} | {'R2 Test':>9}")
    print(f"{'-'*60}")

    for reg in reg_values:
        lr = LinearRegression(
            featuresCol="features",
            labelCol="total_nilai",
            maxIter=100,
            regParam=reg,
            elasticNetParam=0.0
        )
        pipe = Pipeline(stages=[
            kat_idx, ch_idx, assembler, scaler, lr
        ])
        model = pipe.fit(df_train)

        pred_train = model.transform(df_train)
        pred_test  = model.transform(df_test)

        rmse_train = evaluator.setMetricName("rmse").evaluate(pred_train)
        rmse_test  = evaluator.setMetricName("rmse").evaluate(pred_test)
        r2_test    = evaluator.setMetricName("r2").evaluate(pred_test)

        print(f"{reg:>10} | {rmse_train:>14,.0f} | {rmse_test:>14,.0f} | {r2_test:>9.4f}")

    print(f"{'='*60}")
    df_train.unpersist()
    spark.stop()
```

### Langkah A.2 — Jalankan eksperimen regParam

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  --conf spark.sql.shuffle.partitions=20 \
  /tmp/eksplorasi_regparam.py
```

Catat semua baris output pada **Tabel A.1**.

---

## Bagian B — Pengaruh maxDepth pada Decision Tree

### Langkah B.1 — Buat skrip eksperimen maxDepth

```bash
nano /tmp/eksplorasi_maxdepth.py
```

Salin skrip berikut:

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, VectorAssembler, StandardScaler
)
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MaxDepth-Eksplorasi-M9") \
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

    kat_idx   = StringIndexer(inputCol="kategori",
                              outputCol="kat_idx", handleInvalid="keep")
    ch_idx    = StringIndexer(inputCol="channel",
                              outputCol="ch_idx",  handleInvalid="keep")
    label_idx = StringIndexer(inputCol="segmen",
                              outputCol="label",   handleInvalid="keep")
    assembler = VectorAssembler(
        inputCols=["kuantitas","harga_satuan","diskon",
                   "berat_kg","kat_idx","ch_idx"],
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

    depth_values = [2, 4, 6, 8, 10]

    print(f"\n{'='*75}")
    print(f" EKSPERIMEN maxDepth DECISION TREE")
    print(f"{'='*75}")
    print(f"{'Depth':>6} | {'Acc Train':>10} | {'Acc Test':>10} "
          f"| {'F1 Test':>9} | {'Gap':>8} | {'Nodes':>7}")
    print(f"{'-'*75}")

    for depth in depth_values:
        dt = DecisionTreeClassifier(
            featuresCol="features", labelCol="label",
            maxDepth=depth, impurity="gini",
            minInstancesPerNode=5
        )
        pipe = Pipeline(stages=[
            kat_idx, ch_idx, label_idx, assembler, scaler, dt
        ])
        model = pipe.fit(df_train)
        dt_model = model.stages[-1]

        pred_train = model.transform(df_train)
        pred_test  = model.transform(df_test)

        acc_train = mc_eval.setMetricName("accuracy").evaluate(pred_train)
        acc_test  = mc_eval.setMetricName("accuracy").evaluate(pred_test)
        f1_test   = mc_eval.setMetricName("f1").evaluate(pred_test)
        gap       = acc_train - acc_test

        print(f"{depth:>6} | {acc_train:>10.4f} | {acc_test:>10.4f} "
              f"| {f1_test:>9.4f} | {gap:>8.4f} | {dt_model.numNodes:>7}")

    print(f"{'='*75}")
    df_train.unpersist()
    spark.stop()
```

### Langkah B.2 — Jalankan eksperimen maxDepth

```bash
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  --conf spark.sql.shuffle.partitions=20 \
  /tmp/eksplorasi_maxdepth.py
```

Catat semua baris output pada **Tabel B.1**.

---

## Bagian C — Pertanyaan Diskusi Konseptual

Jawab pertanyaan berikut berdasarkan teori di modul dan pengalaman dari Latihan 1–5.

**C.1 — Pada Latihan 2, Decision Tree menghasilkan tabel feature importance. Fitur apa yang paling berpengaruh? Apakah hasilnya masuk akal secara bisnis? Jelaskan mengapa fitur kategorikal (kat_idx, ch_idx) memiliki importance jauh lebih rendah dari fitur numerik seperti harga_satuan.**

> Tulis jawaban Anda di sini:
>
> _..._

---

**C.2 — Pada Latihan 3, K-Means menggunakan silhouette score untuk memilih K optimal. Mengapa kita tidak selalu memilih K yang memberikan inertia terkecil? Apa yang terjadi secara geometris saat K = jumlah data (K = 200 untuk dataset pelanggan)?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**C.3 — Bandingkan arsitektur Pipeline ML (`pyspark.ml.Pipeline`) dengan cara menulis kode ML tanpa Pipeline — memanggil setiap transformer secara manual satu per satu. Berikan dua contoh bug konkret yang bisa muncul pada pendekatan tanpa Pipeline saat model di-deploy ke produksi.**

> Petunjuk: Pikirkan tentang StandardScaler yang di-fit pada data training tetapi tidak disimpan — apa yang terjadi saat data inference baru harus di-transform?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**C.4 — Pada `VectorAssembler`, parameter `handleInvalid="skip"` akan membuang baris dengan nilai null, sedangkan `"keep"` mengisi dengan nilai default (biasanya 0). Dalam dataset transaksi ini (di mana null ditemukan = 0), kapan `"skip"` lebih baik dan kapan `"keep"` lebih baik? Berikan skenario bisnis konkret untuk masing-masing pilihan.**

> Tulis jawaban Anda di sini:
>
> _..._

---

**C.5 — Mengapa `StandardScaler` dengan `withMean=True` tidak direkomendasikan untuk data sparse, seperti hasil `OneHotEncoder`? Apa yang terjadi secara matematis pada representasi sparse vector ketika mean dikurangkan?**

> Petunjuk: Sparse vector menyimpan hanya nilai non-zero secara efisien. Mengurangi mean dari setiap elemen mengubah semua nilai nol menjadi nilai negatif kecil — apa dampaknya terhadap efisiensi memori dan komputasi?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

## Tabel Pencatatan Hasil

### Tabel A.1 — Pengaruh regParam pada Linear Regression

| regParam | RMSE Train | RMSE Test | R² Test | Keterangan |
|---|---|---|---|---|
| 0.0 | _..._ | _..._ | _..._ | Tanpa regularisasi |
| 0.001 | _..._ | _..._ | _..._ | _..._ |
| 0.01 | _..._ | _..._ | _..._ | _..._ |
| 0.1 | _..._ | _..._ | _..._ | Default di skrip Latihan 2 |
| 1.0 | _..._ | _..._ | _..._ | _..._ |
| 10.0 | _..._ | _..._ | _..._ | Regularisasi sangat kuat |

**Pola yang diamati:**

| Pertanyaan | Jawaban |
|---|---|
| regParam berapa menghasilkan RMSE Test terkecil? | _..._ |
| Apakah RMSE Train selalu ≤ RMSE Test? | Ya / Tidak |
| Pada regParam berapa model mulai "terlalu disederhanakan"? | _..._ |
| Tren R² saat regParam naik dari 0 ke 10? | Naik / Turun / Tidak konsisten |

### Tabel B.1 — Pengaruh maxDepth pada Decision Tree

| maxDepth | Acc Train | Acc Test | F1 Test | Gap (Train-Test) | Jumlah Node | Status |
|---|---|---|---|---|---|---|
| 2 | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| 4 | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| 6 | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| 8 | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |
| 10 | _..._ | _..._ | _..._ | _..._ | _..._ | _..._ |

**Pola yang diamati:**

| Pertanyaan | Jawaban |
|---|---|
| Pada depth berapa Acc Test mulai menurun (overfitting mulai)? | _..._ |
| Depth berapa memberikan F1 Test tertinggi? | _..._ |
| Apakah jumlah node selalu berlipat ganda saat depth +2? | Ya / Tidak |
| Gap terbesar (overfitting paling parah) pada depth? | _..._ |

---

## Refleksi dan Analisis

**R5.1 — Dari Tabel A.1, gambarkan secara verbal kurva bias-variance tradeoff yang Anda observasi. Pada regParam = 0, model cenderung mengalami kondisi apa? Pada regParam = 10, kondisi apa yang terjadi? Jelaskan mengapa ada titik "sweet spot" di antaranya.**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R5.2 — Dari Tabel B.1, bandingkan depth=2 dengan depth=10. Pada depth=2 model mengalami underfitting — model terlalu sederhana dengan sedikit node. Jelaskan secara konkret: apa yang terjadi di level pohon ketika depth terlalu rendah? Mengapa model tidak mampu membedakan kelas "rendah" (1.3% data) dari kelas lainnya?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R5.3 — Dari Tabel B.1, jumlah node Decision Tree meningkat secara eksponensial seiring depth bertambah. Dalam konteks komputasi terdistribusi di Spark dengan YARN, apa implikasi jumlah node yang sangat besar (misal depth=15 menghasilkan ribuan node) terhadap penggunaan memori executor dan waktu training?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R5.4 — Dari seluruh rangkaian latihan (1–5), Anda telah membangun 4 model berbeda: Linear Regression, Logistic Regression, Decision Tree, dan K-Means. Jika diberikan tugas baru: "prediksi apakah seorang pelanggan akan melakukan transaksi bernilai di atas Rp 5 juta dalam 30 hari ke depan", model mana yang paling tepat, dan data tambahan apa yang diperlukan di luar dataset transaksi yang ada?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R5.5 — Refleksi akhir: Jelaskan perbedaan mendasar antara melatih model ML "biasa" (scikit-learn di laptop) dengan melatih model ML menggunakan Spark MLlib di atas YARN. Kapan Spark MLlib benar-benar diperlukan, dan kapan ia justru berlebihan (overkill) untuk suatu kasus?**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Tabel Rangkuman Seluruh Latihan Modul 9

Isi tabel ini sebagai ringkasan komprehensif dari Latihan 1 sampai 5.

### Perbandingan Semua Model yang Dilatih

| Model | Tugas | Label | Metrik Utama | Nilai | Waktu Training |
|---|---|---|---|---|---|
| Linear Regression | Regresi | total_nilai (kontinu) | R² | _..._ | _..._ detik |
| Logistic Regression | Klasifikasi 3 kelas | segmen | F1-Score | _..._ | _..._ detik |
| Decision Tree (Lat. 2) | Klasifikasi 3 kelas | segmen | F1-Score | _..._ | _..._ detik |
| Decision Tree (E2E) | Klasifikasi 3 kelas | segmen | Accuracy Test | _..._ | _..._ detik |
| K-Means | Clustering | — (unsupervised) | Silhouette | _..._ | _..._ detik/K |

### Komponen Pipeline yang Digunakan

| Komponen | Tipe | Digunakan Di | Fungsi |
|---|---|---|---|
| `StringIndexer` | Transformer/Estimator | Lat. 2, 3, 4 | _..._ |
| `VectorAssembler` | Transformer | Semua | _..._ |
| `StandardScaler` | Estimator | Semua | _..._ |
| `Imputer` | Estimator | Lat. 4 | _..._ |
| `DecisionTreeClassifier` | Estimator | Lat. 2, 4 | _..._ |
| `LogisticRegression` | Estimator | Lat. 2 | _..._ |
| `LinearRegression` | Estimator | Lat. 2, 5A | _..._ |
| `KMeans` | Estimator | Lat. 3 | _..._ |
| `Pipeline` | Orchestrator | Lat. 2–5 | _..._ |
| `PipelineModel` | Artefak tersimpan | Lat. 4 | _..._ |

### Struktur Data Lake Setelah Latihan Selesai

| Layer | Path HDFS | Isi | Dibuat di Latihan |
|---|---|---|---|
| Bronze | `/datalake/bronze/transaksi/` | 10.000 baris raw | Latihan 1 |
| Silver | `/datalake/silver/transaksi/` | 10.000 baris siap pakai | Latihan 1 |
| Gold | `/datalake/gold/prediksi_segmen/` | Hasil prediksi DT | Latihan 4 |
| Gold | `/datalake/gold/segmentasi_pelanggan/` | Hasil K-Means | Latihan 3 |
| Models | `/models/segmentasi_dt/v1/` | PipelineModel | Latihan 4 |

---

## Kesimpulan Latihan 5

Setelah menyelesaikan seluruh rangkaian latihan Modul 9, lengkapi pernyataan berikut:

> "Dari eksperimen regularisasi (Tabel A.1), regParam optimal adalah **___** yang menghasilkan RMSE test terkecil = **___**. Nilai regParam terlalu tinggi (10.0) menyebabkan model mengalami **___** (underfitting/overfitting), sedangkan regParam = 0 berisiko **___**. Dari eksperimen maxDepth (Tabel B.1), pohon mulai overfitting pada depth = **___**, ditandai dengan gap accuracy sebesar **___**. Depth optimal yang menyeimbangkan Acc Train dan Acc Test adalah **___**. Secara keseluruhan, komponen pipeline yang paling sering menjadi bottleneck komputasi adalah **___** karena **___**."

---

## Penutup Modul 9

Selamat! Anda telah menyelesaikan seluruh rangkaian latihan Modul 9. Berikut ringkasan pencapaian:

| Latihan | Topik Utama | Status |
|---|---|---|
| Latihan 1 | Setup HDFS, eksplorasi dataset, statistik deskriptif, distribusi label | ☐ Selesai |
| Latihan 2 | Linear Regression, Logistic Regression, Decision Tree, feature importance | ☐ Selesai |
| Latihan 3 | Agregasi pelanggan, K-Means, elbow method, profil klaster, Gold layer | ☐ Selesai |
| Latihan 4 | Pipeline E2E, simpan/muat PipelineModel, batch inference, Spark UI | ☐ Selesai |
| Latihan 5 | Regularisasi, maxDepth sweep, bias-variance tradeoff, diskusi konseptual | ☐ Selesai |

---

*Modul 9 — Machine Learning Big Data · Institut Teknologi Sumatera*
