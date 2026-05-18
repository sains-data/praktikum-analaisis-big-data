# Latihan 4 — Pipeline End-to-End Terintegrasi
**Modul 7 · Orkestrasi Alur Kerja dan Tata Kelola Big Data** | Estimasi waktu: **25 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Membangun DAG Airflow yang mengintegrasikan keempat komponen: HDFS (ingest), Spark (ETL), Hive (serving), dan Atlas (governance)
- Merancang pipeline dengan task paralel menggunakan sintaks `>> [task_a, task_b]`
- Mendaftarkan lineage proses Spark ke Atlas secara programatik dari dalam DAG
- Menjalankan Spark agregasi Silver → Gold sebagai tahap akhir pipeline
- Mengamati dan menganalisis perilaku eksekusi paralel di Airflow UI

---

## Prasyarat

- [ ] Latihan 1, 2, dan 3 selesai
- [ ] Data Bronze dan Silver tersedia di HDFS
- [ ] Entitas `transaksi_bronze` dan `transaksi_silver` sudah terdaftar di Atlas
- [ ] File `/opt/spark-jobs/latihan_etl.py` dan `/opt/spark-jobs/pipeline_gold.py` tersedia

---

## Langkah Kerja

### Langkah 4.1 — Pahami arsitektur pipeline sebelum membuat DAG

Pipeline end-to-end ini memiliki pola aliran sebagai berikut:

```
mulai
  │
  ▼
ingest_bronze          ← BashOperator: generate CSV + put ke HDFS
  │
  ▼
spark_etl              ← SparkSubmitOperator: Bronze → Silver
  │
  ├──────────────────┐
  ▼                  ▼
hive_load_silver   registrasi_atlas   ← Paralel: Hive load + Atlas lineage
  │                  │
  └──────────────────┘
            │
            ▼
     spark_gold_agregasi    ← SparkSubmitOperator: Silver → Gold
            │
            ▼
          selesai
```

**Dua task paralel** (`hive_load_silver` dan `registrasi_atlas`) dapat berjalan bersamaan karena keduanya bergantung pada `spark_etl` tetapi tidak saling bergantung satu sama lain. Ini adalah pola "fan-out then fan-in" dalam desain DAG.

---

### Langkah 4.2 — Buat tabel Hive Silver (prasyarat untuk HiveOperator)

Sebelum HiveOperator dapat memuat data, tabel Hive harus ada. Jalankan dari dalam Hive CLI atau beeline:

```bash
# Opsi 1: via hive CLI
hive -e "
CREATE DATABASE IF NOT EXISTS datalake;

CREATE EXTERNAL TABLE IF NOT EXISTS datalake.transaksi_silver (
    id             STRING,
    nilai          DOUBLE,
    kategori       STRING,
    tanggal_proses STRING
)
PARTITIONED BY (tanggal STRING)
STORED AS PARQUET
LOCATION 'hdfs:///datalake/silver/latihan/';
"

# Opsi 2: cek apakah tabel sudah ada
hive -e "SHOW TABLES IN datalake;"
```

Catat nama tabel yang terbentuk pada **Tabel 4.1**.

---

### Langkah 4.3 — Buat DAG pipeline end-to-end

```bash
nano ~/airflow/dags/pipeline_e2e.py
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
import requests
import json

ATLAS_URL  = "http://localhost:21000/api/atlas/v2"
ATLAS_AUTH = ("admin", "admin")
ATLAS_HDR  = {"Content-Type": "application/json"}

default_args = {
    "owner": "mahasiswa",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def daftar_lineage_ke_atlas(**context):
    """
    Task Python: mendaftarkan lineage proses Spark ETL ke Atlas.
    Dipanggil SETELAH spark_etl sukses.
    """
    tanggal = context["ds"]

    payload = {
        "entities": [{
            "typeName": "spark_process",
            "attributes": {
                "name": f"ETL-Bronze-Silver-{tanggal}",
                "qualifiedName":
                    f"spark.etl.bronze_silver.{tanggal}@cluster1",
                "description": (
                    f"Spark ETL: Bronze → Silver, tanggal {tanggal}"
                ),
                "inputs": [{
                    "typeName": "hive_table",
                    "uniqueAttributes": {
                        "qualifiedName":
                            "datalake.transaksi_bronze@cluster1"
                    }
                }],
                "outputs": [{
                    "typeName": "hive_table",
                    "uniqueAttributes": {
                        "qualifiedName":
                            "datalake.transaksi_silver@cluster1"
                    }
                }],
                "userName": "mahasiswa",
            }
        }]
    }

    resp = requests.post(
        f"{ATLAS_URL}/entity/bulk",
        auth=ATLAS_AUTH, headers=ATLAS_HDR,
        data=json.dumps(payload)
    )
    print(f"[ATLAS] Status registrasi lineage: HTTP {resp.status_code}")
    if resp.status_code in (200, 201):
        guids = resp.json().get("guidAssignments", {})
        print(f"[ATLAS] GUID proses ETL: {list(guids.values())}")
        # Push GUID ke XCom untuk referensi
        context["ti"].xcom_push(
            key="atlas_process_guid",
            value=list(guids.values())[0] if guids else None
        )
    else:
        print(f"[ATLAS] Response: {resp.text[:300]}")


def validasi_gold(**context):
    """Verifikasi bahwa Gold layer berhasil dibuat."""
    import subprocess
    result = subprocess.run(
        ["hdfs", "dfs", "-ls", "/datalake/gold/latihan/"],
        capture_output=True, text=True
    )
    lines = [l for l in result.stdout.strip().split("\n") if l.strip()]
    print(f"[VALIDASI GOLD] Baris output: {len(lines)}")
    print(f"[VALIDASI GOLD] Output:\n{result.stdout}")
    atlas_guid = context["ti"].xcom_pull(
        task_ids="registrasi_atlas",
        key="atlas_process_guid"
    )
    print(f"[VALIDASI GOLD] GUID Atlas proses ETL: {atlas_guid}")
    if not result.stdout.strip():
        raise ValueError("Gold layer kosong — spark_gold gagal!")


with DAG(
    dag_id="pipeline_e2e_terintegrasi",
    default_args=default_args,
    description="Pipeline end-to-end: Spark + Hive + Atlas terintegrasi",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["latihan", "modul7", "e2e"],
) as dag:

    mulai = EmptyOperator(task_id="mulai")

    # ── Step 1: Ingest CSV ke HDFS Bronze ───────────────────────
    ingest = BashOperator(
        task_id="ingest_bronze",
        bash_command=(
            "python /opt/scripts/generate_data.py {{ ds }} 100 "
            "> /tmp/transaksi_{{ ds_nodash }}.csv && "
            "hdfs dfs -mkdir -p /datalake/bronze/latihan/ && "
            "hdfs dfs -put -f "
            "/tmp/transaksi_{{ ds_nodash }}.csv "
            "/datalake/bronze/latihan/ && "
            "echo '[INGEST] Selesai. Baris:' "
            "$(wc -l < /tmp/transaksi_{{ ds_nodash }}.csv)"
        ),
    )

    # ── Step 2: Spark ETL Bronze → Silver ───────────────────────
    spark_etl = SparkSubmitOperator(
        task_id="spark_etl",
        application="/opt/spark-jobs/latihan_etl.py",
        conn_id="spark_default",
        application_args=["{{ ds }}"],
        conf={"spark.sql.shuffle.partitions": "10"},
        executor_memory="512m",
        num_executors=2,
        name="airflow-etl-e2e-{{ ds_nodash }}",
        verbose=True,
    )

    # ── Step 3a: Load ke Hive Silver (paralel dengan Atlas) ─────
    hive_load = BashOperator(
        task_id="hive_load_silver",
        bash_command=(
            "hive -e \""
            "MSCK REPAIR TABLE datalake.transaksi_silver;"
            "SELECT COUNT(*) FROM datalake.transaksi_silver;\""
            " && echo '[HIVE] Repair table selesai.'"
        ),
    )

    # ── Step 3b: Daftarkan lineage ke Atlas (paralel dengan Hive)
    atlas_register = PythonOperator(
        task_id="registrasi_atlas",
        python_callable=daftar_lineage_ke_atlas,
        provide_context=True,
    )

    # ── Step 4: Spark Gold agregasi Silver → Gold ────────────────
    spark_gold = SparkSubmitOperator(
        task_id="spark_gold_agregasi",
        application="/opt/spark-jobs/pipeline_gold.py",
        conn_id="spark_default",
        application_args=["{{ ds }}"],
        conf={"spark.sql.shuffle.partitions": "10"},
        executor_memory="512m",
        num_executors=2,
        name="airflow-gold-e2e-{{ ds_nodash }}",
        verbose=True,
    )

    # ── Step 5: Validasi output Gold ─────────────────────────────
    validasi = PythonOperator(
        task_id="validasi_output_gold",
        python_callable=validasi_gold,
        provide_context=True,
    )

    selesai = EmptyOperator(task_id="selesai")

    # ── Definisi dependensi (termasuk task paralel) ──────────────
    #
    # mulai → ingest → spark_etl → [hive_load, atlas_register]
    #                                        │
    #                               spark_gold_agregasi
    #                                        │
    #                               validasi_output_gold → selesai
    #
    (mulai
     >> ingest
     >> spark_etl
     >> [hive_load, atlas_register]   # fan-out: paralel
     >> spark_gold                     # fan-in: menunggu keduanya
     >> validasi
     >> selesai)
```

Simpan file.

---

### Langkah 4.4 — Trigger DAG dan pantau eksekusi

```bash
# Verifikasi DAG terdeteksi
sleep 10
airflow dags list | grep pipeline_e2e

# Trigger
airflow dags trigger pipeline_e2e_terintegrasi

echo "Pantau di: http://localhost:8080"
echo "DAG: pipeline_e2e_terintegrasi"
```

Pantau via CLI setiap 30 detik:

```bash
RUN_ID=$(airflow dags list-runs pipeline_e2e_terintegrasi \
  --output json 2>/dev/null | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print(d[0]['run_id'])" 2>/dev/null)

watch -n 30 "airflow tasks states-for-dag-run \
  pipeline_e2e_terintegrasi '$RUN_ID'"
```

---

### Langkah 4.5 — Amati tab Graph di Airflow UI

Buka `http://localhost:8080` → DAG `pipeline_e2e_terintegrasi` → tab **Graph**.

**Yang harus diamati:**

1. Apakah terlihat pola "fan-out" dari `spark_etl` ke dua task paralel?
2. Apakah `hive_load_silver` dan `registrasi_atlas` terhubung dari node yang sama?
3. Apakah `spark_gold_agregasi` baru berjalan setelah **keduanya** selesai (fan-in)?
4. Warna setiap node setelah eksekusi selesai

Catat pada **Tabel 4.2**.

---

### Langkah 4.6 — Baca log task penting

```bash
# Log task registrasi_atlas (lihat respons Atlas API)
airflow tasks logs pipeline_e2e_terintegrasi \
  registrasi_atlas "$RUN_ID" 1

# Log task spark_gold_agregasi (lihat output agregasi)
airflow tasks logs pipeline_e2e_terintegrasi \
  spark_gold_agregasi "$RUN_ID" 1
```

Catat pada **Tabel 4.3**.

---

### Langkah 4.7 — Verifikasi output di HDFS dan Atlas

```bash
# Cek Silver layer
echo "=== Silver Layer ==="
hdfs dfs -ls /datalake/silver/latihan/
hdfs dfs -du -s -h /datalake/silver/latihan/

# Cek Gold layer
echo -e "\n=== Gold Layer ==="
hdfs dfs -ls /datalake/gold/latihan/
hdfs dfs -du -s -h /datalake/gold/latihan/

# Cek lineage di Atlas (jalankan script dari Latihan 3)
echo -e "\n=== Lineage di Atlas ==="
python3 /tmp/ambil_lineage.py
```

Catat pada **Tabel 4.3**.

---

### Langkah 4.8 — Verifikasi lineage di Atlas Web UI

Buka `http://localhost:21000` → Search → hive_table → cari `transaksi_silver`.

1. Klik tab **Lineage**
2. Amati apakah sekarang ada node baru yang muncul (proses ETL sebagai node penghubung)
3. Klik node proses ETL (jika ada) — lihat propertinya

Catat pada **Tabel 4.4**.

---

## Tabel Pencatatan Hasil

### Tabel 4.1 — Status Infrastruktur Sebelum Pipeline

| Komponen | Status | Keterangan |
|---|---|---|
| Tabel Hive `datalake.transaksi_silver` | Ada / Tidak | _..._ |
| Data di HDFS Bronze | Ada / Tidak | _..._ baris |
| Data di HDFS Silver (dari Latihan 2) | Ada / Tidak | _..._ file |
| Entitas di Atlas (Bronze + Silver) | Ada / Tidak | _..._ entitas |
| Koneksi `spark_default` di Airflow | Ada / Tidak | _..._ |

### Tabel 4.2 — Status dan Durasi Setiap Task

| Task | Status | Durasi (detik) | Berjalan Paralel Dengan |
|---|---|---|---|
| `mulai` | _..._ | _..._ | — |
| `ingest_bronze` | _..._ | _..._ | — |
| `spark_etl` | _..._ | _..._ | — |
| `hive_load_silver` | _..._ | _..._ | `registrasi_atlas` |
| `registrasi_atlas` | _..._ | _..._ | `hive_load_silver` |
| `spark_gold_agregasi` | _..._ | _..._ | — |
| `validasi_output_gold` | _..._ | _..._ | — |
| `selesai` | _..._ | _..._ | — |
| **Total pipeline** | — | **_..._ detik** | — |

**Task pertama yang dimulai setelah `spark_etl` selesai:** _..._
**Task kedua yang dimulai bersamaan:** _..._
**Selisih waktu mulai kedua task paralel:** _..._ detik

### Tabel 4.3 — Verifikasi Output dan Log

**HDFS setelah pipeline selesai:**

| Layer | Path | Jumlah File | Ukuran Total |
|---|---|---|---|
| Bronze | `/datalake/bronze/latihan/` | _..._ | _..._ |
| Silver | `/datalake/silver/latihan/` | _..._ | _..._ |
| Gold | `/datalake/gold/latihan/` | _..._ | _..._ |

**Log task `registrasi_atlas`:**

| Informasi | Nilai |
|---|---|
| HTTP status respons Atlas | _..._ |
| GUID proses ETL yang dibuat | _..._ |
| Apakah GUID berhasil di-push ke XCom? | Ya / Tidak |

**Output agregasi Spark Gold (dari log `spark_gold_agregasi`):**

| Kategori | Jumlah Transaksi | Total Nilai | Rata-rata Nilai |
|---|---|---|---|
| ELEKTRONIK | _..._ | _..._ | _..._ |
| FASHION | _..._ | _..._ | _..._ |
| MAKANAN | _..._ | _..._ | _..._ |
| KESEHATAN | _..._ | _..._ | _..._ |
| OTOMOTIF | _..._ | _..._ | _..._ |

### Tabel 4.4 — Pengamatan Airflow UI dan Atlas UI

**Airflow UI — tab Graph:**

| Pengamatan | Jawaban |
|---|---|
| Apakah pola fan-out terlihat dari `spark_etl`? | Ya / Tidak |
| Berapa task yang terhubung ke `spark_etl` sebagai downstream? | _..._ |
| Apakah `spark_gold` baru aktif setelah KEDUANYA selesai? | Ya / Tidak |
| Task terlama dan durasinya | _..._ (_..._ detik) |
| Task tercepat (selain EmptyOperator) | _..._ (_..._ detik) |

**Atlas UI — setelah pipeline:**

| Pengamatan | Sebelum Pipeline (Lat. 3) | Setelah Pipeline (Lat. 4) |
|---|---|---|
| Jumlah entitas di Atlas | _..._ | _..._ |
| Apakah entitas `spark_process` muncul? | Tidak | Ya / Tidak |
| Tab Lineage `transaksi_silver` — ada relasi? | _..._ | _..._ |
| Visualisasi lineage menampilkan node apa saja? | — | _..._ |

---

## Refleksi dan Analisis

**R4.1 — Dari Tabel 4.2, task `hive_load_silver` dan `registrasi_atlas` dirancang untuk berjalan paralel menggunakan sintaks `>> [hive_load, atlas_register]`. Apa prasyarat desain yang memungkinkan keduanya berjalan paralel? Berikan contoh skenario di mana dua task yang terlihat "independen" ternyata tidak aman dijalankan paralel.**

> Petunjuk: Dua task aman dijalankan paralel jika tidak ada dependensi data di antara mereka — output satu bukan input yang lain. Tapi pikirkan tentang resource contention (memori YARN) atau konflik akses file yang sama di HDFS.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.2 — Dari Tabel 4.4, setelah `registrasi_atlas` berjalan, entitas `spark_process` muncul di Atlas yang menghubungkan Bronze (input) dan Silver (output). Mengapa pendaftaran lineage ini penting untuk impact analysis? Berikan skenario konkret menggunakan dataset transaksi ini.**

> Petunjuk: Bayangkan kolom `nilai` di Bronze harus diubah tipenya dari `string` ke `integer`. Dengan lineage yang tercatat, bagaimana Anda menelusuri dampaknya ke downstream?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.3 — Dari Tabel 4.2, `spark_gold_agregasi` hanya bisa dimulai setelah KEDUANYA (`hive_load_silver` dan `registrasi_atlas`) selesai — ini adalah pola "fan-in". Jika `hive_load_silver` membutuhkan 120 detik tetapi `registrasi_atlas` hanya 5 detik, kapan `spark_gold` bisa mulai? Bagaimana pola ini berbeda dari menjalankan semua task secara serial?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.4 — Dari Tabel 4.3, output Gold layer berisi agregasi per kategori. Jika pipeline ini dijalankan lagi keesokan harinya dengan data baru, apakah nilai agregasi di Gold akan ditambahkan ke data hari ini, atau menggantikannya? Bagaimana Anda mendesain pipeline agar Gold layer menyimpan data historis per hari (bukan overwrite)?**

> Petunjuk: Pikirkan tentang `mode("append")` vs `mode("overwrite")`, atau partisi berdasarkan `tanggal_proses`.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.5 — Pipeline end-to-end ini menggabungkan empat komponen: HDFS/Hive (penyimpanan), Spark (komputasi), Airflow (orkestrasi), dan Atlas (governance). Jika salah satu komponen mengalami downtime — misalnya Atlas server tidak bisa dijangkau — apakah seluruh pipeline harus gagal? Bagaimana Anda merancang DAG agar kegagalan di task `registrasi_atlas` tidak menghentikan eksekusi `spark_gold`?**

> Petunjuk: Airflow memiliki parameter `trigger_rule` di setiap task. Nilai default-nya adalah `all_success`, tetapi bisa diubah menjadi `all_done`, `one_success`, atau `none_failed_min_one_success`.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 4

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Pipeline end-to-end berhasil dijalankan dengan total **___** task, di mana **___** task berjalan secara paralel (fan-out). Total durasi pipeline adalah **___** detik — **___** detik lebih cepat dibandingkan jika semua task dijalankan serial, karena penjematan waktu dari paralelisasi sebesar **___** detik. Lineage dari Bronze ke Silver kini terdaftar di Atlas via entitas **___** yang menghubungkan tabel input dan output. Gold layer berhasil dibuat dengan **___** kategori produk, di mana kategori dengan total nilai tertinggi adalah **___** (Rp **___**)."

---

*Latihan 4 selesai. Lanjutkan ke **Latihan 5 — Eksplorasi Lanjutan: Retry, Pencarian Atlas, dan Diskusi**.*