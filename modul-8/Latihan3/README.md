# Latihan 3 — Spark Structured Streaming: Agregasi Window
**Modul 8 · Analitik Aliran Data** | Estimasi waktu: **35 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Membangun pipeline Spark Structured Streaming yang membaca dari Kafka
- Mem-parse payload JSON menggunakan schema eksplisit
- Menerapkan tumbling window dengan watermark pada data streaming
- Menjalankan dua query streaming secara paralel dengan output mode berbeda
- Membaca dan menginterpretasikan metrik dari Spark UI (Structured Streaming tab)

---

## Prasyarat

- [ ] Latihan 1 dan 2 sudah selesai
- [ ] Docker dan Kafka masih berjalan (`docker compose ps`)
- [ ] Java 11 terpasang (`java -version`)
- [ ] PySpark terpasang (`pip install pyspark==3.5.5`)
- [ ] Koneksi internet tersedia (untuk mengunduh paket `spark-sql-kafka` pertama kali)

---

## Langkah Kerja

### Langkah 3.1 — Pastikan producer aktif

Buka Terminal 1 dan jalankan producer (jika belum berjalan dari Latihan 2):

```bash
python scripts/producer_transaksi.py
```

Biarkan berjalan di background. Producer **harus aktif** agar Spark memiliki data untuk diproses.

---

### Langkah 3.2 — Tinjau kode Spark Streaming sebelum dijalankan

Buka `spark/streaming_agregasi.py` dan pahami tiga bagian utamanya:

**Bagian A — Membaca dari Kafka:**
```python
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", TOPIC_IN) \
    .option("startingOffsets", "latest") \       # ← hanya event baru
    .option("maxOffsetsPerTrigger", 100) \        # ← maks 100 event per batch
    .option("failOnDataLoss", "false") \
    .load()
```

**Bagian B — Parsing JSON dan Query 1 (tumbling window):**
```python
df_q1 = df \
    .withWatermark("event_time", "2 minutes") \   # ← tunggu max 2 menit late data
    .groupBy(
        F.window("event_time", "1 minute"),        # ← window 1 menit
        "channel"
    ) \
    .agg(
        F.count("*").alias("jumlah"),
        F.sum("amount").alias("total"),
        F.avg("amount").alias("rata_rata"),
    )
```

**Bagian C — Query 2 (global groupBy tanpa window):**
```python
df_q2 = df \
    .groupBy("user_id") \
    .agg(
        F.count("*").alias("total_transaksi"),
        F.sum("amount").alias("total_amount"),
    ) \
    .orderBy(F.col("total_amount").desc())
```

> **Perbedaan kunci:** Query 1 menggunakan `outputMode("update")` karena hanya baris yang berubah yang ditulis. Query 2 menggunakan `outputMode("complete")` karena agregasi global harus selalu menulis ulang seluruh hasil.

---

### Langkah 3.3 — Jalankan pipeline Spark

Buka Terminal 2 (baru, terpisah dari terminal producer) dan jalankan:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --master local[2] \
  --conf spark.sql.shuffle.partitions=4 \
  spark/streaming_agregasi.py
```

> **Pertama kali dijalankan:** Spark akan mengunduh paket `spark-sql-kafka` (~20 MB). Proses ini membutuhkan 1–5 menit tergantung kecepatan internet. Unduhan berikutnya langsung dari cache.

Tunggu hingga muncul pesan:

```
[Streaming] Pipeline aktif.
[Streaming] Spark UI → http://localhost:4040
[Streaming] Kafka UI → http://localhost:8080
[Streaming] Tekan Ctrl+C untuk berhenti.
```

---

### Langkah 3.4 — Amati output Query 1 di terminal

Setelah ~15 detik, output Query 1 akan muncul di terminal. Contoh:

```
-------------------------------------------
Batch: 2
-------------------------------------------
+-------------------+-------------------+---------+------+----------+---------+
|window_start       |window_end         |channel  |jumlah|total     |rata_rata|
+-------------------+-------------------+---------+------+----------+---------+
|2024-04-15 10:01:00|2024-04-15 10:02:00|mobile   |8     |6421000.0 |802625.0 |
|2024-04-15 10:01:00|2024-04-15 10:02:00|web      |5     |3150000.0 |630000.0 |
|2024-04-15 10:01:00|2024-04-15 10:02:00|atm      |3     |890000.0  |296667.0 |
+-------------------+-------------------+---------+------+----------+---------+
```

Catat output dari **3 batch berturut-turut** pada **Tabel 3.1**.

---

### Langkah 3.5 — Amati output Query 2 di terminal

Setelah ~30 detik, output Query 2 akan muncul. Contoh:

```
-------------------------------------------
Batch: 1
-------------------------------------------
+----------+----------------+------------------+
|user_id   |total_transaksi |total_amount      |
+----------+----------------+------------------+
|usr-0033  |12              |24500000.0        |
|usr-0007  |10              |19800000.0        |
|usr-0042  |9               |17200000.0        |
+----------+----------------+------------------+
```

Catat top-5 user pada **Tabel 3.2**.

---

### Langkah 3.6 — Pantau Spark UI

Buka browser ke `http://localhost:4040` dan navigasi ke tab **Structured Streaming**.

Anda akan melihat dua query aktif:

```
penjualan_per_channel    ACTIVE    ...
top_user                 ACTIVE    ...
```

Klik pada query `penjualan_per_channel` untuk melihat grafik detail.

Amati nilai berikut dan catat pada **Tabel 3.3**:
- **Input Rate** (events/sec)
- **Processing Rate** (events/sec)
- **Input Rows** per trigger
- **Batch Duration** (ms)

---

### Langkah 3.7 — Amati watermark di log Spark

Di terminal Spark, cari baris log yang menyebutkan watermark. Jalankan perintah berikut di terminal terpisah untuk memfilter log:

```bash
# Jika Spark menghasilkan log ke file, grep watermark:
# Atau amati langsung di terminal Spark — cari baris seperti:
# "Filtering rows older than ... (watermark)"
```

Anda juga bisa melihat nilai watermark saat ini di Spark UI:

**Structured Streaming → penjualan_per_channel → State Operator**

Catat nilai watermark pada **Tabel 3.3**.

---

### Langkah 3.8 — Biarkan pipeline berjalan minimal 3 menit

Amati perubahan output setiap batch. Perhatikan:
- Window mana yang pertama kali "ditutup" (tidak muncul lagi di output update)
- Apakah ada window baru yang terbuka seiring waktu berjalan

Setelah 3 menit, lanjutkan ke pencatatan hasil.

---

## Tabel Pencatatan Hasil

### Tabel 3.1 — Output Query 1: Penjualan per Channel per Window

Isi dari **3 batch berturut-turut** Query 1 (pilih satu window yang sama untuk dibandingkan):

| Batch ke- | Window Start | Window End | Channel | Jumlah Transaksi | Total Penjualan | Rata-rata |
|---|---|---|---|---|---|---|
| 1 | _..._ | _..._ | mobile | _..._ | _..._ | _..._ |
| 1 | _..._ | _..._ | web | _..._ | _..._ | _..._ |
| 1 | _..._ | _..._ | atm | _..._ | _..._ | _..._ |
| 2 | _..._ | _..._ | mobile | _..._ | _..._ | _..._ |
| 2 | _..._ | _..._ | web | _..._ | _..._ | _..._ |
| 3 | _..._ | _..._ | mobile | _..._ | _..._ | _..._ |

**Channel dengan total tertinggi secara keseluruhan:** _..._

### Tabel 3.2 — Output Query 2: Top 5 User berdasarkan Total Penjualan

*(catat dari batch terakhir Query 2 yang muncul)*

| Rank | user_id | Total Transaksi | Total Amount (Rp) |
|---|---|---|---|
| 1 | _..._ | _..._ | _..._ |
| 2 | _..._ | _..._ | _..._ |
| 3 | _..._ | _..._ | _..._ |
| 4 | _..._ | _..._ | _..._ |
| 5 | _..._ | _..._ | _..._ |

### Tabel 3.3 — Metrik dari Spark UI (Structured Streaming)

| Metrik | Nilai yang Tercatat | Waktu Pencatatan |
|---|---|---|
| Input Rate (events/sec) | _..._ | _HH:MM_ |
| Processing Rate (events/sec) | _..._ | _HH:MM_ |
| Avg Batch Duration (ms) | _..._ | _HH:MM_ |
| Total Input Rows (kumulatif) | _..._ | _HH:MM_ |
| Watermark event_time saat ini | _..._ | _HH:MM_ |
| Jumlah window aktif di state | _..._ | _HH:MM_ |

### Tabel 3.4 — Perbandingan Output Mode

| Aspek | Query 1 (update) | Query 2 (complete) |
|---|---|---|
| Output mode | update | complete |
| Frekuensi trigger | 15 detik | 30 detik |
| Baris yang ditulis per batch | Hanya yang berubah | Seluruh hasil |
| Apakah baris lama muncul ulang? | Ya/Tidak | Ya/Tidak |
| Jumlah baris rata-rata per batch | _..._ | _..._ |

---

## Refleksi dan Analisis

**R3.1 — Pada Tabel 3.1, amati nilai `total` untuk channel yang sama antara Batch 1 dan Batch 2 (dalam window yang sama). Apakah nilainya bertambah atau tetap? Jelaskan mengapa demikian dalam konteks `outputMode("update")`.**

> Petunjuk: Update mode hanya menulis baris yang berubah sejak batch sebelumnya.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.2 — Mengapa Query 2 (top user) harus menggunakan `outputMode("complete")` dan tidak bisa menggunakan `outputMode("update")`?**

> Petunjuk: Pikirkan tentang operasi `orderBy()` pada streaming query — ini membutuhkan semua data untuk menentukan urutan.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.3 — Dari Tabel 3.3, bandingkan nilai Input Rate dan Processing Rate. Apa yang terjadi jika Processing Rate secara konsisten lebih rendah dari Input Rate? Apa dampaknya terhadap sistem?**

> Petunjuk: Pikirkan tentang backpressure dan lag.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.4 — Pipeline menggunakan `maxOffsetsPerTrigger = 100`. Jelaskan apa yang terjadi jika producer mengirim 500 event dalam 15 detik (interval satu trigger). Berapa event yang akan diproses di trigger pertama? Sisa 400 event ke mana?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.5 — Dari hasil Query 1, apakah ada channel yang secara konsisten menghasilkan total penjualan tertinggi? Apakah ini mencerminkan kondisi nyata atau hanya efek dari data acak (random)? Bagaimana cara memvalidasi pola seperti ini dalam sistem produksi?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.6 — Watermark diset ke `"2 minutes"`. Artinya Spark menunggu 2 menit setelah event time terbaru sebelum menutup sebuah window. Jika ada event yang tiba terlambat 3 menit (event_time-nya 3 menit lebih lama dari watermark saat ini), apa yang terjadi pada event tersebut?**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 3

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Pipeline Spark Structured Streaming berhasil membaca event dari Kafka dan melakukan agregasi dengan dua query paralel. Query 1 menggunakan **___** window selama **___** menit dengan watermark **___** menit. Output mode yang digunakan adalah **___** sehingga hanya baris yang **___** yang ditulis setiap trigger. Query 2 menggunakan mode **___** karena membutuhkan **___** data untuk operasi pengurutan. Rata-rata throughput pemrosesan adalah **___** events/detik."

---

*Latihan 3 selesai. Lanjutkan ke **Latihan 4 — Fault Tolerance: Uji Checkpoint dan Recovery**.*