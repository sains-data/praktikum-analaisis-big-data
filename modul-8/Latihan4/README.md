# Latihan 4 — Fault Tolerance: Uji Checkpoint dan Recovery
**Modul 8 · Analitik Aliran Data** | Estimasi waktu: **15 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Memverifikasi struktur direktori checkpoint yang dibuat Spark
- Mensimulasikan kegagalan job streaming dengan menghentikan paksa Spark
- Mengamati bahwa Kafka menyimpan event selama periode producer berjalan
- Membuktikan bahwa Spark melanjutkan dari offset checkpoint, bukan dari awal
- Menganalisis konsistensi state agregasi sebelum dan sesudah recovery

---

## Prasyarat

- [ ] Latihan 3 sudah selesai
- [ ] Pipeline Spark dari Latihan 3 masih berjalan (atau siap dijalankan ulang)
- [ ] Producer Python masih aktif di Terminal 1
- [ ] Checkpoint tersimpan di `/tmp/checkpoints/streaming-agregasi/`

---

## Langkah Kerja

### Langkah 4.1 — Catat state terakhir sebelum shutdown

Sebelum menghentikan Spark, catat nilai-nilai berikut dari **batch terakhir** yang muncul di terminal:

- Nomor batch terakhir (misal: `Batch: 7`)
- Total penjualan channel **mobile** dari batch terakhir
- Total penjualan channel **web** dari batch terakhir
- Top-1 user dari Query 2

Catat pada **Tabel 4.1 kolom "Sebelum Restart"**.

---

### Langkah 4.2 — Hentikan paksa Spark

Di Terminal 2 (terminal Spark), tekan:

```
Ctrl+C
```

Amati pesan shutdown yang muncul. Contoh:

```
^C[Streaming] Menghentikan query...
24/04/15 10:15:33 INFO StreamingQueryManager: Query penjualan_per_channel was stopped
24/04/15 10:15:33 INFO StreamingQueryManager: Query top_user was stopped
```

> **Penting:** Biarkan producer di Terminal 1 **tetap berjalan** selama proses ini. Kafka terus menerima event meskipun Spark berhenti — inilah yang akan kita verifikasi.

Catat waktu shutdown pada **Tabel 4.2**.

---

### Langkah 4.3 — Verifikasi isi direktori checkpoint

Jalankan perintah berikut di terminal baru (Terminal 3):

```bash
# Lihat struktur direktori checkpoint
ls -la /tmp/checkpoints/streaming-agregasi/
```

Output yang diharapkan:
```
drwxr-xr-x  q1/
drwxr-xr-x  q2/
```

```bash
# Lihat isi checkpoint Query 1
ls -la /tmp/checkpoints/streaming-agregasi/q1/
```

Output yang diharapkan:
```
drwxr-xr-x  commits/
drwxr-xr-x  metadata
drwxr-xr-x  offsets/
drwxr-xr-x  state/
```

```bash
# Lihat file offset yang tersimpan
ls -la /tmp/checkpoints/streaming-agregasi/q1/offsets/
```

Output contoh:
```
-rw-r--r--  0
-rw-r--r--  1
-rw-r--r--  2
-rw-r--r--  3
...
-rw-r--r--  7
```

Setiap file merepresentasikan satu micro-batch yang sudah selesai diproses. Nama file adalah nomor batch.

```bash
# Baca isi file offset terakhir (ganti angka sesuai batch terakhir)
cat /tmp/checkpoints/streaming-agregasi/q1/offsets/7
```

Output contoh (JSON):
```json
{"batchWatermarkMs":1713176460000,"batchTimestampMs":1713176475000,
"conf":{"spark.sql.streaming.stateStore.providerClass":"..."},
"sources":[{"description":"KafkaV2[Subscribe[transaksi-stream]]",
"startOffset":{"transaksi-stream":{"2":89,"1":84,"0":87}},
"endOffset":{"transaksi-stream":{"2":112,"1":108,"0":110}}}]}
```

Catat nilai offset akhir dari setiap partisi pada **Tabel 4.2**.

---

### Langkah 4.4 — Verifikasi event yang masuk selama Spark berhenti

Cek offset terbaru di Kafka (producer masih berjalan):

```bash
docker exec kafka-broker kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream
```

Bandingkan hasilnya dengan offset terakhir di file checkpoint. Selisihnya adalah jumlah event yang masuk **saat Spark berhenti**.

Catat pada **Tabel 4.2**.

---

### Langkah 4.5 — Restart pipeline Spark

Kembali ke Terminal 2 dan jalankan ulang **perintah yang sama persis**:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --master local[2] \
  --conf spark.sql.shuffle.partitions=4 \
  spark/streaming_agregasi.py
```

Perhatikan log awal saat startup. Cari baris yang menyebutkan **checkpoint** atau **resuming**:

```
INFO MicroBatchExecution: Resuming at batch 8. Checkpoint file = /tmp/checkpoints/streaming-agregasi/q1
INFO KafkaDataConsumer: Starting offset for transaksi-stream-0: 110
```

Catat waktu restart dan nomor batch pertama pasca-restart pada **Tabel 4.3**.

---

### Langkah 4.6 — Amati batch pertama setelah restart

Tunggu batch pertama muncul di terminal (~15 detik). Perhatikan:

1. Nomor batch — apakah melanjutkan dari batch sebelumnya atau mulai dari 0?
2. Window yang muncul — apakah mencakup event yang masuk **selama Spark berhenti**?
3. State Query 2 — apakah total penjualan per user bertambah dari sebelum shutdown?

Catat pada **Tabel 4.3 kolom "Setelah Restart"**.

---

### Langkah 4.7 — Verifikasi tidak ada event yang hilang

Hitung total event yang seharusnya diproses:

```
Total event = Offset Kafka saat ini - Offset checkpoint awal (saat Spark pertama kali start)
```

Bandingkan dengan total input rows di Spark UI (`http://localhost:4040`):

**Structured Streaming → penjualan_per_channel → Total Input Rows**

Apakah angkanya konsisten?

---

## Tabel Pencatatan Hasil

### Tabel 4.1 — State Agregasi Sebelum dan Sesudah Restart

| Metrik | Sebelum Restart | Setelah Restart (batch pertama) | Keterangan |
|---|---|---|---|
| Nomor batch terakhir/pertama | Batch: _..._ | Batch: _..._ | Melanjutkan atau reset? |
| Total penjualan — channel `mobile` | Rp _..._ | Rp _..._ | Bertambah / sama / reset? |
| Total penjualan — channel `web` | Rp _..._ | Rp _..._ | Bertambah / sama / reset? |
| Total penjualan — channel `atm` | Rp _..._ | Rp _..._ | Bertambah / sama / reset? |
| Top-1 user (Query 2) | _usr-xxxx_ | _usr-xxxx_ | Berubah / sama? |
| Total transaksi top-1 user | _..._ | _..._ | Bertambah? |

### Tabel 4.2 — Snapshot Offset saat Shutdown

| Informasi | Nilai |
|---|---|
| Waktu Spark dihentikan (Ctrl+C) | _HH:MM:SS_ |
| Nomor batch terakhir yang selesai | _..._ |
| Offset checkpoint — Partisi 0 | _..._ |
| Offset checkpoint — Partisi 1 | _..._ |
| Offset checkpoint — Partisi 2 | _..._ |
| Offset Kafka terbaru — Partisi 0 (setelah berhenti) | _..._ |
| Offset Kafka terbaru — Partisi 1 (setelah berhenti) | _..._ |
| Offset Kafka terbaru — Partisi 2 (setelah berhenti) | _..._ |
| **Selisih (event masuk saat Spark berhenti)** | **_..._** |

### Tabel 4.3 — Pengamatan Pasca-Restart

| Pengamatan | Nilai/Jawaban |
|---|---|
| Waktu Spark direstart | _HH:MM:SS_ |
| Durasi Spark berhenti | _menit:detik_ |
| Nomor batch pertama setelah restart | _..._ |
| Apakah ada pesan "Resuming" di log? | Ya / Tidak |
| Apakah offset dimulai dari checkpoint? | Ya / Tidak |
| Apakah ada event yang terlewat (hilang)? | Ya / Tidak |
| Apakah ada duplikat event yang diproses? | Ya / Tidak |

### Tabel 4.4 — Struktur Direktori Checkpoint

| Subdirektori/File | Fungsi |
|---|---|
| `offsets/` | _Isi dengan penjelasan Anda_ |
| `commits/` | _Isi dengan penjelasan Anda_ |
| `state/` | _Isi dengan penjelasan Anda_ |
| `metadata` | _Isi dengan penjelasan Anda_ |

---

## Refleksi dan Analisis

**R4.1 — Dari Tabel 4.2, ada sejumlah event yang masuk ke Kafka selama Spark berhenti. Mengapa event-event ini tidak hilang? Komponen mana yang bertanggung jawab menjaga event tersebut tetap tersedia?**

> Petunjuk: Ingat filosofi Kafka sebagai distributed commit log dengan retensi data.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.2 — Dari Tabel 4.3, nomor batch setelah restart melanjutkan dari nomor sebelum shutdown (bukan mulai dari 0). Mengapa ini penting untuk konsistensi state agregasi? Apa yang terjadi jika Spark memulai ulang nomor batch dari 0?**

> Petunjuk: Pikirkan tentang idempotency dan bagaimana Spark membedakan "sudah diproses" dari "belum diproses".
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.3 — Dari Tabel 4.1, total penjualan per channel setelah restart lebih tinggi dari sebelum restart (karena mencakup event yang masuk saat Spark berhenti). Apakah ini menunjukkan bahwa tidak ada event yang hilang? Jelaskan alur recovery secara lengkap dari perspektif Kafka offset.**

> Petunjuk: Trace alur: checkpoint offset → Kafka → batch pertama pasca-restart.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.4 — File checkpoint disimpan di `/tmp/` yang merupakan direktori sementara dan akan hilang saat server di-reboot. Dalam lingkungan produksi, di mana seharusnya checkpoint disimpan, dan mengapa?**

> Petunjuk: Lihat kembali kode di modul — ada contoh menggunakan path `hdfs:///checkpoints/...`
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.5 — Sebutkan skenario kegagalan nyata (minimal 3 skenario) yang bisa terjadi dalam sistem streaming produksi, dan jelaskan bagaimana mekanisme checkpoint + Kafka retention menangani masing-masing skenario tersebut.**

> Contoh skenario: driver crash, executor OOM, network partition, dll.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R4.6 — Pada tabel ringkasan fault tolerance di modul, consumer (sink) membutuhkan "idempoten write / upsert" untuk menjamin exactly-once efek di tujuan. Mengapa checkpoint saja tidak cukup untuk menjamin exactly-once di sisi sink? Apa yang bisa terjadi di "jeda" antara pemrosesan selesai dan checkpoint tersimpan?**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 4

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Saat Spark dihentikan selama **___** detik, Kafka tetap menerima **___** event baru. Setelah restart, Spark membaca checkpoint dari batch **___** dan melanjutkan pemrosesan dari offset **___** tanpa kehilangan event. Mekanisme ini memungkinkan jaminan **___** (at-most-once / at-least-once / exactly-once) dalam pipeline streaming. Komponen yang menjaga event tidak hilang saat Spark berhenti adalah **___** dengan kemampuan **___** (data retention / partitioning)."

---

*Latihan 4 selesai. Lanjutkan ke **Latihan 5 — Eksplorasi: Windowing dan Delivery Semantics**.*