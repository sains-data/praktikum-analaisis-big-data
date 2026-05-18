# Latihan 2 — Integrasi Airflow dengan Apache Spark
**Modul 7 · Orkestrasi Alur Kerja dan Tata Kelola Big Data** | Estimasi waktu: **25 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Membuat Spark job Python untuk transformasi data CSV (Bronze) menjadi Parquet (Silver)
- Mengintegrasikan Spark job ke dalam DAG Airflow menggunakan `SparkSubmitOperator`
- Mengonfigurasi koneksi `spark_default` yang digunakan oleh SparkSubmitOperator
- Membaca dan menginterpretasikan log Spark job dari dalam Airflow UI
- Memverifikasi output Parquet di HDFS Silver layer

---

## Prasyarat

- [ ] Latihan 1 selesai dan DAG `latihan_pipeline_transaksi` berstatus success
- [ ] Data tersedia di `hdfs:///datalake/bronze/latihan/`
- [ ] Koneksi `spark_default` terdaftar di Airflow (`airflow connections get spark_default`)
- [ ] File `/opt/spark-jobs/latihan_etl.py` sudah tersedia (dari setup lab)

---

## Langkah Kerja

### Langkah 2.1 — Verifikasi Spark job tersedia

```bash
# Cek file Spark ETL job
ls -lh /opt/spark-jobs/latihan_etl.py

# Cek isi singkat
head -30 /opt/spark-jobs/latihan_etl.py
```

Jika file belum ada, buat ulang dari panduan setup lab (Langkah Step 3).

Verifikasi juga koneksi Spark di Airflow:

```bash
source ~/airflow-env/bin/activate
airflow connections get spark_default
```

Output yang diharapkan menampilkan `conn_type = spark` dan `host = yarn`.

---

### Langkah 2.2 — Uji Spark job secara manual (tanpa Airflow)

Sebelum diintegrasikan ke DAG, uji Spark job secara langsung untuk memastikan logic ETL benar:

```bash
# Pastikan ada data di Bronze layer
hdfs dfs -ls /datalake/bronze/latihan/

# Jalankan Spark ETL secara manual
spark-submit \
  --master yarn \
  --deploy-mode client \
  --executor-memory 512m \
  --num-executors 2 \
  --conf spark.sql.shuffle.partitions=10 \
  /opt/spark-jobs/latihan_etl.py \
  $(date +%Y-%m-%d)
```

Amati output di terminal. Catat nilai-nilai berikut pada **Tabel 2.1**:
- `[ETL] Baris raw`
- `[ETL] Baris valid`
- `[ETL] Baris ditolak`

Verifikasi output di Silver layer:

```bash
hdfs dfs -ls /datalake/silver/latihan/
hdfs dfs -du -s -h /datalake/silver/latihan/
```

---

### Langkah 2.3 — Buat DAG baru dengan SparkSubmitOperator

```bash
nano ~/airflow/dags/latihan_pipeline_spark.py
```

Salin kode berikut:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit \
    import SparkSubmitOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "mahasiswa",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def validasi_output_silver(**context):
    """Verifikasi bahwa Spark berhasil menulis output ke HDFS."""
    result = subprocess.run(
        ["hdfs", "dfs", "-ls", "/datalake/silver/latihan/"],
        capture_output=True, text=True
    )
    lines = result.stdout.strip().split("\n")
    parquet_files = [l for l in lines if ".parquet" in l or "part-" in l]
    print(f"[VALIDASI OUTPUT] File di Silver layer: {len(lines) - 1}")
    print(f"[VALIDASI OUTPUT] File Parquet: {len(parquet_files)}")
    print(f"[VALIDASI OUTPUT] Output lengkap:\n{result.stdout}")
    if not result.stdout.strip():
        raise ValueError(
            "Silver layer kosong — Spark ETL mungkin gagal!"
        )
    context["ti"].xcom_push(
        key="jumlah_file_silver", value=len(lines) - 1
    )


def cetak_ringkasan(**context):
    """Cetak ringkasan eksekusi pipeline."""
    n_file = context["ti"].xcom_pull(
        task_ids="validasi_output_silver",
        key="jumlah_file_silver"
    )
    tanggal = context["ds"]
    print(f"[RINGKASAN] Tanggal  : {tanggal}")
    print(f"[RINGKASAN] File Silver: {n_file}")
    print(f"[RINGKASAN] Pipeline Spark ETL SELESAI.")


with DAG(
    dag_id="latihan_pipeline_spark",
    default_args=default_args,
    description="DAG latihan Airflow + Spark ETL",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["latihan", "modul7", "spark"],
) as dag:

    mulai = EmptyOperator(task_id="mulai")

    # Task 1: Generate dan ingest data ke Bronze
    buat_dan_ingest = BashOperator(
        task_id="buat_dan_ingest_bronze",
        bash_command=(
            # Generate CSV
            "python /opt/scripts/generate_data.py {{ ds }} 100 "
            "> /tmp/transaksi_{{ ds_nodash }}.csv && "
            # Ingest ke HDFS Bronze
            "hdfs dfs -mkdir -p /datalake/bronze/latihan/ && "
            "hdfs dfs -put -f "
            "/tmp/transaksi_{{ ds_nodash }}.csv "
            "/datalake/bronze/latihan/ && "
            "echo 'Ingest selesai:' && "
            "hdfs dfs -ls /datalake/bronze/latihan/"
        ),
    )

    # Task 2: Spark ETL Bronze → Silver (menggunakan SparkSubmitOperator)
    spark_etl = SparkSubmitOperator(
        task_id="spark_etl_bronze_silver",
        application="/opt/spark-jobs/latihan_etl.py",
        conn_id="spark_default",
        application_args=["{{ ds }}"],
        conf={
            "spark.executor.memory": "512m",
            "spark.sql.shuffle.partitions": "10",
        },
        executor_memory="512m",
        num_executors=2,
        verbose=True,     # ← tampilkan log Spark di Airflow log
        name="airflow-etl-{{ ds_nodash }}",
    )

    # Task 3: Validasi output Silver
    validasi = PythonOperator(
        task_id="validasi_output_silver",
        python_callable=validasi_output_silver,
        provide_context=True,
    )

    # Task 4: Cetak ringkasan
    ringkasan = PythonOperator(
        task_id="cetak_ringkasan",
        python_callable=cetak_ringkasan,
        provide_context=True,
    )

    selesai = EmptyOperator(task_id="selesai")

    # Definisi dependensi
    mulai >> buat_dan_ingest >> spark_etl >> validasi >> ringkasan >> selesai
```

Simpan file.

---

### Langkah 2.4 — Trigger DAG Spark dan pantau eksekusi

```bash
# Verifikasi DAG terdeteksi
sleep 10
airflow dags list | grep latihan_pipeline_spark

# Trigger DAG
airflow dags trigger latihan_pipeline_spark

echo "DAG triggered. Pantau di http://localhost:8080"
echo "atau via CLI:"
echo "  airflow dags list-runs latihan_pipeline_spark"
```

Pantau perkembangan setiap 30 detik (task Spark lebih lambat dari BashOperator):

```bash
# Gunakan watch untuk update otomatis
watch -n 30 "airflow dags list-runs latihan_pipeline_spark"
```

Tekan `Ctrl+C` setelah DAG run berstatus `success` atau `failed`.

---

### Langkah 2.5 — Baca log task Spark ETL

```bash
RUN_ID=$(airflow dags list-runs latihan_pipeline_spark \
  --output json 2>/dev/null | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print(d[0]['run_id'])" 2>/dev/null)

echo "Run ID: $RUN_ID"

# Log task Spark ETL (berisi output Spark)
airflow tasks logs latihan_pipeline_spark \
  spark_etl_bronze_silver "$RUN_ID" 1
```

Di dalam log ini, cari baris-baris penting:
- `[ETL] Memproses data tanggal: ...`
- `[ETL] Baris raw    : ...`
- `[ETL] Baris valid  : ...`
- `[ETL] Baris ditolak: ...`
- `[ETL] Output ditulis ke: hdfs:///datalake/silver/latihan/`

Catat pada **Tabel 2.2**.

---

### Langkah 2.6 — Amati tab Graph di Airflow UI

Buka `http://localhost:8080`, navigasi ke DAG `latihan_pipeline_spark`, klik tab **Graph**.

**Yang diamati:**
1. Jumlah total node dalam DAG
2. Warna setiap node setelah eksekusi
3. Apakah alur linear dari kiri ke kanan?
4. Klik node `spark_etl_bronze_silver` → klik **Log** → amati log Spark yang panjang

Catat pada **Tabel 2.3**.

---

### Langkah 2.7 — Verifikasi output Silver layer di HDFS

```bash
# Lihat daftar file di Silver
hdfs dfs -ls /datalake/silver/latihan/

# Ukuran total Silver layer
hdfs dfs -du -s -h /datalake/silver/latihan/

# Baca beberapa baris dari file Parquet (konversi ke text via Spark)
pyspark --master yarn --executor-memory 512m --num-executors 1 << 'EOF'
df = spark.read.parquet("hdfs:///datalake/silver/latihan/")
print(f"Total baris Silver: {df.count()}")
df.show(5, truncate=False)
df.printSchema()
exit()
EOF
```

Catat hasil pada **Tabel 2.1**.

---

## Tabel Pencatatan Hasil

### Tabel 2.1 — Hasil Spark ETL (dari pengujian manual + Airflow)

| Metrik | Nilai dari Manual (`spark-submit`) | Nilai dari Airflow DAG |
|---|---|---|
| Baris raw dibaca dari Bronze | _..._ | _..._ |
| Baris valid ditulis ke Silver | _..._ | _..._ |
| Baris ditolak (null/negatif) | _..._ | _..._ |
| Persentase data ditolak | _..._% | _..._% |
| Jumlah file di Silver layer | _..._ | _..._ |
| Ukuran total Silver layer | _..._ KB/MB | _..._ KB/MB |
| Schema Silver (kolom-kolom) | _..._ | — |

**Apakah hasil manual dan via Airflow konsisten?** Ya / Tidak — Catatan: _..._

### Tabel 2.2 — Durasi dan Status Setiap Task (DAG Airflow)

| Task | Status | Durasi (detik) | Baris Kunci dari Log |
|---|---|---|---|
| `mulai` | _..._ | _..._ | — |
| `buat_dan_ingest_bronze` | _..._ | _..._ | _..._ |
| `spark_etl_bronze_silver` | _..._ | _..._ | `[ETL] Baris valid: ...` |
| `validasi_output_silver` | _..._ | _..._ | `[VALIDASI OUTPUT] File: ...` |
| `cetak_ringkasan` | _..._ | _..._ | `[RINGKASAN] File Silver: ...` |
| `selesai` | _..._ | _..._ | — |
| **Total pipeline** | — | **_..._ detik** | — |

### Tabel 2.3 — Pengamatan Airflow UI: DAG Spark

| Aspek | Nilai |
|---|---|
| Jumlah node di tab Graph | _..._ |
| Task terlama dan durasinya | `spark_etl_bronze_silver` / _..._ detik |
| Apakah log Spark terlihat di Airflow log? | Ya / Tidak |
| Baris pertama log Spark yang informatif | _..._ |
| Apakah output Silver terverifikasi di task validasi? | Ya / Tidak |
| Jumlah file XCom yang dibuat di run ini | _..._ |

### Tabel 2.4 — Perbandingan Eksekusi Manual vs via Airflow

| Aspek | Eksekusi Manual (`spark-submit`) | Via Airflow (`SparkSubmitOperator`) |
|---|---|---|
| Cara memulai | Ketik perintah di terminal | Trigger DAG via CLI/UI |
| Log tersimpan di | Terminal (hilang setelah sesi) | Airflow log (persisten, bisa diakses UI) |
| Retry otomatis jika gagal | Tidak | Ya (sesuai konfigurasi `retries`) |
| Monitoring status | Manual (pantau terminal) | Real-time di Airflow UI |
| Dependensi task berikutnya | Manual (jalankan manual) | Otomatis (dependensi DAG) |

---

## Refleksi dan Analisis

**R2.1 — Dari Tabel 2.1, berapa persen data yang ditolak oleh Spark ETL? Generator data di-desain untuk menghasilkan ~3% baris tidak valid (id kosong atau nilai negatif). Apakah persentase yang Anda amati konsisten? Jelaskan mengapa pendekatan "reject dan log" lebih baik daripada "tolak seluruh batch jika ada satu baris tidak valid".**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.2 — Dari Tabel 2.2, task `spark_etl_bronze_silver` adalah task terlama. Mengapa ada overhead waktu yang signifikan antara trigger Airflow dan saat Spark benar-benar mulai memproses data? Sebutkan setidaknya tiga tahap yang terjadi sebelum Spark memulai eksekusi aktual.**

> Petunjuk: YARN resource negotiation, JVM startup, executor launch, JAR distribution ke worker nodes.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.3 — Dari Tabel 2.4, log Spark job via Airflow tersimpan secara persisten dan bisa diakses kapan saja dari UI. Dalam konteks tim data engineering yang beranggotakan 10 orang, jelaskan konkret bagaimana persistensi log ini mengubah cara debugging dibandingkan eksekusi manual via `spark-submit` di terminal.**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.4 — Spark ETL menulis output ke Silver layer menggunakan `mode("overwrite")`. Jelaskan mengapa sifat idempotent ini penting untuk pipeline yang dijalankan oleh Airflow secara terjadwal. Apa risiko jika menggunakan `mode("append")` dalam konteks pipeline harian?**

> Petunjuk: Bayangkan pipeline berjalan dua kali pada hari yang sama karena ada retry atau re-trigger manual.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.5 — `SparkSubmitOperator` dikonfigurasi dengan `num_executors=2` dan `executor_memory="512m"`. Dalam lingkungan lab single-node, berapa total memori yang dialokasikan ke Spark? Apa yang terjadi jika konfigurasi ini dinaikan menjadi `num_executors=8` dan `executor_memory="2g"` pada mesin dengan 8 GB RAM?**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 2

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Pipeline Spark ETL berhasil diintegrasikan ke Airflow menggunakan **___**. Dari **___** baris raw, **___** baris valid ditulis ke Silver layer dalam format **___** (Parquet/CSV). Persentase data ditolak sebesar **___**%, yang disebabkan oleh **___**. Task `spark_etl_bronze_silver` membutuhkan **___** detik — jauh lebih lama dari task lain karena **___**. Dibandingkan eksekusi manual, keunggulan utama menjalankan Spark via Airflow adalah **___** dan **___**."

---

*Latihan 2 selesai. Lanjutkan ke **Latihan 3 — Menjelajahi Apache Atlas**.*