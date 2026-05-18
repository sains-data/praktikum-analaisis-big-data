# Latihan 1 — Membuat DAG Airflow Sederhana
**Modul 7 · Orkestrasi Alur Kerja dan Tata Kelola Big Data** | Estimasi waktu: **20 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Mendefinisikan DAG Airflow menggunakan Python dengan `BashOperator` dan `PythonOperator`
- Memahami konsep dependensi antar-task menggunakan operator `>>`
- Menggunakan XCom untuk mengirim nilai dari satu task ke task lain
- Mengaktifkan dan memonitor DAG melalui Airflow Web UI (Graph, Grid, Log)
- Membaca log eksekusi setiap task untuk memverifikasi keberhasilan

---

## Prasyarat

- [ ] Kontainer `bigdata-spark` berjalan (`docker ps`)
- [ ] Airflow scheduler dan webserver aktif (`pgrep -f "airflow scheduler"` mengembalikan PID)
- [ ] Airflow UI dapat diakses di `http://localhost:8080` (login: `admin / admin123`)
- [ ] Direktori `~/airflow/dags/` sudah ada
- [ ] Script `/opt/scripts/generate_data.py` sudah tersedia

---

## Langkah Kerja

### Langkah 1.1 — Verifikasi Airflow berjalan

Di dalam kontainer `bigdata-spark`, aktifkan virtual environment dan verifikasi:

```bash
source ~/airflow-env/bin/activate

# Cek scheduler
pgrep -f "airflow scheduler" > /dev/null && echo "Scheduler: BERJALAN" || echo "Scheduler: MATI"

# Cek webserver
pgrep -f "airflow webserver" > /dev/null && echo "Webserver: BERJALAN" || echo "Webserver: MATI"
```

Jika salah satu tidak berjalan, nyalakan kembali:

```bash
airflow scheduler > ~/airflow/logs/scheduler.log 2>&1 &
airflow webserver --port 8080 > ~/airflow/logs/webserver.log 2>&1 &
sleep 30 && curl -s http://localhost:8080/health
```

---

### Langkah 1.2 — Buat file DAG

```bash
nano ~/airflow/dags/latihan_pipeline.py
```

Salin kode berikut secara lengkap:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "mahasiswa",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def validasi_data(**context):
    """Fungsi Python: cek apakah file hasil ingest ada."""
    tanggal = context["ds_nodash"]
    path = f"/tmp/transaksi_{tanggal}.csv"
    if not os.path.exists(path):
        raise FileNotFoundError(f"File tidak ditemukan: {path}")
    with open(path) as f:
        baris = f.readlines()
    jumlah = len(baris) - 1   # minus header
    print(f"[VALIDASI] Ditemukan {jumlah} baris data.")
    context["ti"].xcom_push(key="jumlah_baris", value=jumlah)


def cetak_laporan(**context):
    """Fungsi Python: cetak ringkasan eksekusi menggunakan XCom."""
    jumlah = context["ti"].xcom_pull(
        task_ids="validasi_data",
        key="jumlah_baris"
    )
    print(f"[LAPORAN] Pipeline selesai. Total baris: {jumlah}")


with DAG(
    dag_id="latihan_pipeline_transaksi",
    default_args=default_args,
    description="DAG latihan modul 7 - pipeline sederhana",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["latihan", "modul7"],
) as dag:

    mulai = EmptyOperator(task_id="mulai")

    buat_file = BashOperator(
        task_id="buat_file_simulasi",
        bash_command=(
            "python /opt/scripts/generate_data.py "
            "{{ ds }} 100 "
            "> /tmp/transaksi_{{ ds_nodash }}.csv "
            "&& echo 'File berhasil dibuat: '"
            "$(wc -l < /tmp/transaksi_{{ ds_nodash }}.csv)"
            "' baris (termasuk header)'"
        ),
    )

    validasi = PythonOperator(
        task_id="validasi_data",
        python_callable=validasi_data,
        provide_context=True,
    )

    ingest_hdfs = BashOperator(
        task_id="ingest_ke_hdfs",
        bash_command=(
            "hdfs dfs -mkdir -p /datalake/bronze/latihan/ && "
            "hdfs dfs -put -f /tmp/transaksi_{{ ds_nodash }}.csv "
            "/datalake/bronze/latihan/ && "
            "echo 'Ingest selesai.' && "
            "hdfs dfs -ls /datalake/bronze/latihan/"
        ),
    )

    laporan = PythonOperator(
        task_id="cetak_laporan",
        python_callable=cetak_laporan,
        provide_context=True,
    )

    selesai = EmptyOperator(task_id="selesai")

    # Definisi urutan eksekusi (dependensi task)
    mulai >> buat_file >> validasi >> ingest_hdfs >> laporan >> selesai
```

Simpan: `Ctrl+O` → `Enter` → `Ctrl+X`

---

### Langkah 1.3 — Verifikasi DAG terdeteksi oleh scheduler

```bash
# Tunggu scheduler membaca file baru (~5-15 detik)
sleep 10

# Cek apakah DAG muncul di daftar
airflow dags list | grep latihan_pipeline_transaksi
```

Output yang diharapkan:

```
latihan_pipeline_transaksi | DAG latihan modul 7 | False | ...
```

Jika tidak muncul, cek apakah ada error syntax di file DAG:

```bash
python ~/airflow/dags/latihan_pipeline.py
# Harus tidak ada output error
```

---

### Langkah 1.4 — Trigger DAG secara manual

```bash
# Trigger DAG untuk dijalankan sekarang
airflow dags trigger latihan_pipeline_transaksi

# Tunggu beberapa detik
sleep 5

# Lihat daftar DAG runs
airflow dags list-runs latihan_pipeline_transaksi
```

Catat **run_id** yang muncul — formatnya seperti `manual__2024-xx-xxT00:00:00+00:00`. Catat pada **Tabel 1.1**.

---

### Langkah 1.5 — Pantau status task via CLI

```bash
# Ganti run_id dengan nilai yang diperoleh dari langkah sebelumnya
RUN_ID=$(airflow dags list-runs latihan_pipeline_transaksi \
  --output json 2>/dev/null | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print(d[0]['run_id'])" 2>/dev/null)

echo "Run ID: $RUN_ID"

# Pantau status semua task (jalankan berulang setiap 15 detik)
watch -n 15 "airflow tasks states-for-dag-run latihan_pipeline_transaksi '$RUN_ID'"
```

Tekan `Ctrl+C` setelah semua task berstatus `success`. Catat pada **Tabel 1.2**.

---

### Langkah 1.6 — Baca log setiap task

```bash
# Log task buat_file_simulasi
airflow tasks logs latihan_pipeline_transaksi \
  buat_file_simulasi "$RUN_ID" 1

# Log task validasi_data
airflow tasks logs latihan_pipeline_transaksi \
  validasi_data "$RUN_ID" 1

# Log task cetak_laporan (berisi nilai XCom)
airflow tasks logs latihan_pipeline_transaksi \
  cetak_laporan "$RUN_ID" 1
```

Catat output penting dari setiap log pada **Tabel 1.2**.

---

### Langkah 1.7 — Verifikasi melalui Airflow Web UI

Buka browser ke `http://localhost:8080`. Login dengan `admin / admin123`.

Di halaman **DAGs**, cari `latihan_pipeline_transaksi` dan klik namanya.

**Amati tab Graph:**
1. Apakah visualisasi menampilkan 6 node yang terhubung secara linear?
2. Node mana yang berwarna hijau (sukses)?
3. Apakah ada node yang berwarna merah (gagal) atau kuning (running)?

**Amati tab Grid:**
1. Apakah semua task di baris teratas berstatus hijau?
2. Berapa total durasi eksekusi dari kiri ke kanan?

**Amati detail task `validasi_data`:**
1. Klik node `validasi_data` di tab Graph
2. Klik **Log**
3. Cari baris `[VALIDASI] Ditemukan ... baris data.`

**Amati XCom dari task `cetak_laporan`:**
1. Klik node `cetak_laporan` di tab Graph
2. Klik **XCom** (atau lihat di log)
3. Verifikasi nilai `jumlah_baris` diterima dengan benar

Catat semua pengamatan pada **Tabel 1.3**.

---

### Langkah 1.8 — Verifikasi file di HDFS

```bash
hdfs dfs -ls /datalake/bronze/latihan/
hdfs dfs -cat /datalake/bronze/latihan/transaksi_$(date +%Y%m%d).csv | head -5
```

Catat path dan isi file pada **Tabel 1.1**.

---

## Tabel Pencatatan Hasil

### Tabel 1.1 — Informasi Eksekusi DAG

| Informasi | Nilai yang Tercatat |
|---|---|
| Run ID DAG | _..._ |
| Tanggal eksekusi (`ds`) | _..._ |
| Nama file CSV yang dibuat | `/tmp/transaksi_` _..._ `.csv` |
| Jumlah baris di file CSV (termasuk header) | _..._ |
| Path HDFS tempat file disimpan | _..._ |
| Apakah file CSV terlihat di HDFS? | Ya / Tidak |

### Tabel 1.2 — Status dan Durasi Setiap Task

| Task | Status Akhir | Durasi (detik) | Pesan Penting dari Log |
|---|---|---|---|
| `mulai` | _success/failed_ | _..._ | _(EmptyOperator, tidak ada log)_ |
| `buat_file_simulasi` | _..._ | _..._ | _..._ |
| `validasi_data` | _..._ | _..._ | `[VALIDASI] Ditemukan ... baris` |
| `ingest_ke_hdfs` | _..._ | _..._ | _..._ |
| `cetak_laporan` | _..._ | _..._ | `[LAPORAN] Pipeline selesai. Total baris: ...` |
| `selesai` | _..._ | _..._ | _(EmptyOperator, tidak ada log)_ |
| **Total durasi pipeline** | — | **_..._ detik** | — |

### Tabel 1.3 — Pengamatan Airflow Web UI

| Aspek yang Diamati | Hasil |
|---|---|
| Jumlah node di tab Graph | _..._ |
| Bentuk alur (linear / bercabang / gabungan) | _..._ |
| Warna semua node setelah eksekusi | _..._ |
| Jumlah baris yang tervalidasi (dari log validasi_data) | _..._ |
| Nilai XCom `jumlah_baris` yang diterima task laporan | _..._ |
| Apakah nilai XCom sama dengan jumlah baris validasi? | Ya / Tidak |
| Task dengan durasi terpanjang | _..._ (_..._ detik) |
| Task dengan durasi terpendek | _..._ (_..._ detik) |

### Tabel 1.4 — Analisis Struktur DAG

Gambarkan struktur dependensi DAG secara teks berdasarkan apa yang Anda lihat di tab Graph:

```
mulai → ___ → ___ → ___ → ___ → selesai
```

*(isi titik-titik dengan nama task yang benar)*

---

## Refleksi dan Analisis

**R1.1 — DAG menggunakan `EmptyOperator` untuk task `mulai` dan `selesai` meskipun keduanya tidak melakukan komputasi apapun. Dari perspektif desain pipeline, apa fungsi arsitektural dari task-task ini? Berikan satu skenario konkret di mana task `mulai` bisa diisi dengan sesuatu yang lebih berguna.**

> Petunjuk: Pikirkan tentang pengiriman notifikasi, pengecekan pre-condition, atau persiapan environment sebelum pipeline dimulai.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.2 — Dari Tabel 1.2, task `ingest_ke_hdfs` menggunakan `BashOperator` dengan perintah `hdfs dfs -put`. Mengapa pipeline menggunakan pendekatan "buat file CSV dulu di `/tmp/`, lalu `put` ke HDFS" alih-alih langsung menulis ke HDFS dari script generator? Apa kelebihan dan kekurangan pendekatan ini?**

> Petunjuk: Pikirkan tentang atomicity (apakah file yang setengah-tertulis di HDFS lebih berbahaya dari file di `/tmp/`?), visibilitas error, dan kemampuan debugging.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.3 — Mekanisme XCom digunakan untuk mengirim nilai `jumlah_baris` dari task `validasi_data` ke task `cetak_laporan`. Dari Tabel 1.3, verifikasi apakah nilai yang dikirim sama dengan yang diterima. Jelaskan bagaimana XCom berbeda dari variabel global Python biasa dalam konteks eksekusi Airflow yang terdistribusi.**

> Petunjuk: Dalam Airflow mode CeleryExecutor atau KubernetesExecutor, setiap task bisa berjalan di worker yang berbeda. Mengapa variabel Python biasa tidak bisa dipakai dalam skenario ini?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.4 — DAG ini menggunakan `schedule_interval="@daily"` dengan `catchup=False`. Jika `catchup=True` dan DAG baru diaktifkan hari ini padahal `start_date` adalah satu bulan lalu, apa yang terjadi? Kapan `catchup=True` diperlukan dan kapan `catchup=False` lebih aman?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.5 — Dari Tabel 1.2, bandingkan durasi task `buat_file_simulasi` (BashOperator) dengan task `validasi_data` (PythonOperator). Secara umum, operator mana yang cenderung lebih cepat startup-nya dan mengapa? Apa implikasinya saat merancang pipeline dengan ratusan task kecil?**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 1

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "DAG `latihan_pipeline_transaksi` berhasil dieksekusi dengan total **___** task dalam waktu **___** detik. Task `validasi_data` menemukan **___** baris data dan mengirimkan nilai ini ke task `cetak_laporan` melalui mekanisme **___** (XCom/variabel global). Alur eksekusi bersifat **___** (linear/paralel) karena seluruh task dihubungkan dengan operator `>>`  yang mendefinisikan **___** (dependensi/prioritas) antar-task. Dibandingkan cron job, keunggulan utama Airflow yang terlihat dalam latihan ini adalah **___**."

---

*Latihan 1 selesai. Lanjutkan ke **Latihan 2 — Integrasi Airflow dengan Apache Spark**.*