# Latihan 5 — Eksplorasi: Windowing dan Delivery Semantics
**Modul 8 · Analitik Aliran Data** | Estimasi waktu: **10 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Membandingkan perilaku tumbling window dan sliding window secara eksperimental
- Menjelaskan mengapa sliding window menghasilkan lebih banyak baris output
- Menganalisis delivery semantics melalui eksperimen consumer dengan commit manual
- Menghubungkan temuan eksperimen dengan teori at-least-once semantics
- Menjawab pertanyaan diskusi konseptual tentang desain sistem streaming

---

## Prasyarat

- [ ] Latihan 1–4 sudah selesai
- [ ] Docker dan Kafka masih berjalan
- [ ] Producer Python aktif (atau bisa dijalankan ulang)

---

## Bagian A — Sliding Window vs. Tumbling Window

### Langkah A.1 — Hentikan pipeline lama (jika masih berjalan)

Di terminal Spark, tekan `Ctrl+C`. Kita akan menjalankan script baru.

Hapus checkpoint lama agar tidak konflik:

```bash
rm -rf /tmp/checkpoints/exp_a/
```

---

### Langkah A.2 — Pastikan producer aktif

```bash
python scripts/producer_transaksi.py
```

Biarkan berjalan di Terminal 1.

---

### Langkah A.3 — Buat script perbandingan window

Buat file baru:

```bash
nano /tmp/window_comparison.py
```

Salin kode berikut:

```python
"""
window_comparison.py
Membandingkan tumbling window dan sliding window secara bersamaan.
Jalankan dengan spark-submit (lihat instruksi di bawah).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType,
)

spark = SparkSession.builder \
    .appName("WindowComparison") \
    .master("local[2]") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
    ) \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("channel",    StringType(),   True),
    StructField("amount",     DoubleType(),   True),
    StructField("event_time", TimestampType(), True),
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transaksi-stream") \
    .option("startingOffsets", "latest") \
    .load()

df = df_raw.select(
    F.from_json(F.col("value").cast("string"), schema).alias("d")
).select("d.*")

# --- Tumbling Window: 2 menit, tidak tumpang tindih ---
df_tumbling = df \
    .withWatermark("event_time", "3 minutes") \
    .groupBy(F.window("event_time", "2 minutes")) \
    .agg(
        F.count("*").alias("jumlah"),
        F.sum("amount").alias("total_tumbling"),
    ) \
    .select(
        F.col("window.start").alias("w_start"),
        F.col("window.end").alias("w_end"),
        "jumlah",
        F.round("total_tumbling", 0).alias("total"),
    )

# --- Sliding Window: 2 menit lebar, slide setiap 1 menit ---
df_sliding = df \
    .withWatermark("event_time", "3 minutes") \
    .groupBy(F.window("event_time", "2 minutes", "1 minute")) \
    .agg(
        F.count("*").alias("jumlah"),
        F.sum("amount").alias("total_sliding"),
    ) \
    .select(
        F.col("window.start").alias("w_start"),
        F.col("window.end").alias("w_end"),
        "jumlah",
        F.round("total_sliding", 0).alias("total"),
    )

# --- Output ke console ---
q_tumbling = df_tumbling.writeStream \
    .queryName("tumbling_2min") \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoints/exp_a/tumbling") \
    .trigger(processingTime="20 seconds") \
    .start()

q_sliding = df_sliding.writeStream \
    .queryName("sliding_2min_1min") \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "/tmp/checkpoints/exp_a/sliding") \
    .trigger(processingTime="20 seconds") \
    .start()

print("\n[WindowComparison] Dua query aktif.")
print("[WindowComparison] Tumbling: trigger 20 detik")
print("[WindowComparison] Sliding : trigger 20 detik")
print("[WindowComparison] Spark UI: http://localhost:4040")
print("[WindowComparison] Tekan Ctrl+C untuk berhenti.\n")

spark.streams.awaitAnyTermination()
```

---

### Langkah A.4 — Jalankan script

Di Terminal 2:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --master local[2] \
  --conf spark.sql.shuffle.partitions=4 \
  /tmp/window_comparison.py
```

---

### Langkah A.5 — Amati output selama 4 menit

Perhatikan:
1. Berapa baris yang muncul pada output **tumbling** per trigger?
2. Berapa baris yang muncul pada output **sliding** per trigger?
3. Untuk periode waktu yang sama (misal 10:00–10:02), apakah nilai total berbeda antara tumbling dan sliding?

Catat pada **Tabel A.1** dan **Tabel A.2**.

Hentikan dengan `Ctrl+C` setelah 4 menit.

---

## Bagian B — Analisis Delivery Semantics

### Langkah B.1 — Buat script consumer analisis

```bash
nano /tmp/consumer_semantics.py
```

Salin kode berikut:

```python
"""
consumer_semantics.py
Membaca event dari Kafka dan menghitung duplikat untuk
menganalisis delivery semantics at-least-once.
Jalankan: python /tmp/consumer_semantics.py
"""

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "transaksi-stream",
    bootstrap_servers=["localhost:9092"],
    group_id="analisis-duplikat-lab",
    auto_offset_reset="earliest",          # baca dari awal
    enable_auto_commit=False,              # commit manual (at-least-once)
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

seen_ids    = set()
total       = 0
duplikat    = 0
per_channel = {}

TARGET = 100   # baca 100 event
print(f"[Consumer] Membaca {TARGET} event dari topic 'transaksi-stream'...\n")

for msg in consumer:
    event    = msg.value
    event_id = event.get("event_id", "")
    channel  = event.get("channel", "unknown")
    total   += 1

    # Hitung per channel
    per_channel[channel] = per_channel.get(channel, 0) + 1

    if event_id in seen_ids:
        duplikat += 1
        print(f"  [DUPLIKAT TERDETEKSI] event_id={event_id} "
              f"| partition={msg.partition} | offset={msg.offset}")
    else:
        seen_ids.add(event_id)

    # Commit setelah setiap pesan (at-least-once)
    consumer.commit()

    if total >= TARGET:
        break

print(f"\n{'='*50}")
print(f" RINGKASAN ANALISIS DELIVERY SEMANTICS")
print(f"{'='*50}")
print(f" Total event dibaca : {total}")
print(f" Event unik (unik ID): {len(seen_ids)}")
print(f" Duplikat terdeteksi: {duplikat}")
print(f" Rasio duplikat     : {duplikat/total*100:.1f}%")
print(f"\n Distribusi per channel:")
for ch, count in sorted(per_channel.items(), key=lambda x: -x[1]):
    print(f"   {ch:<10} : {count:>4} event ({count/total*100:.1f}%)")
print(f"{'='*50}")

consumer.close()
```

---

### Langkah B.2 — Jalankan consumer analisis

```bash
python /tmp/consumer_semantics.py
```

Tunggu hingga 100 event terbaca dan ringkasan muncul. Catat semua angka pada **Tabel B.1**.

---

### Langkah B.3 — Jalankan ulang consumer (group yang sama)

Jalankan ulang script yang sama **tanpa perubahan**:

```bash
python /tmp/consumer_semantics.py
```

Perhatikan apakah consumer membaca event yang sama (karena offset sudah di-commit) atau membaca event baru dari offset terakhir.

Catat perbedaan pada **Tabel B.2**.

---

## Tabel Pencatatan Hasil

### Tabel A.1 — Output Tumbling Window (2 menit)

*(catat dari minimal 3 trigger berbeda)*

| Trigger ke- | Waktu Trigger | Jumlah Baris Output | Rentang Window yang Muncul |
|---|---|---|---|
| 1 | _HH:MM:SS_ | _..._ | _..._ |
| 2 | _HH:MM:SS_ | _..._ | _..._ |
| 3 | _HH:MM:SS_ | _..._ | _..._ |
| **Rata-rata baris per trigger** | — | **_..._** | — |

### Tabel A.2 — Output Sliding Window (2 menit, slide 1 menit)

*(catat dari minimal 3 trigger berbeda)*

| Trigger ke- | Waktu Trigger | Jumlah Baris Output | Rentang Window yang Muncul |
|---|---|---|---|
| 1 | _HH:MM:SS_ | _..._ | _..._ |
| 2 | _HH:MM:SS_ | _..._ | _..._ |
| 3 | _HH:MM:SS_ | _..._ | _..._ |
| **Rata-rata baris per trigger** | — | **_..._** | — |

### Tabel A.3 — Perbandingan Tumbling vs Sliding

| Aspek Perbandingan | Tumbling (2 menit) | Sliding (2 menit / 1 menit) |
|---|---|---|
| Rata-rata baris per trigger | _..._ | _..._ |
| Satu event masuk ke berapa window? | **1** | **_..._ (maks 2)** |
| Apakah window bisa overlap? | Tidak | Ya / Tidak |
| Frekuensi window baru dibuat | Setiap 2 menit | Setiap _..._ menit |
| Overhead komputasi relatif | Lebih rendah / tinggi | Lebih rendah / tinggi |

### Tabel B.1 — Ringkasan Analisis Delivery Semantics (Run ke-1)

| Metrik | Nilai |
|---|---|
| Total event dibaca | _..._ |
| Event ID unik | _..._ |
| Duplikat terdeteksi | _..._ |
| Rasio duplikat | _..._ % |
| Channel dengan event terbanyak | _..._ |
| Persentase channel terbanyak | _..._ % |

### Tabel B.2 — Perbandingan Run ke-1 vs Run ke-2

| Aspek | Run ke-1 | Run ke-2 |
|---|---|---|
| Offset awal pembacaan | _..._ | _..._ |
| Apakah membaca event yang sama? | Ya / Tidak | — |
| Total event dibaca | 100 | _..._ |
| Apakah ada duplikat antar-run? | — | Ya / Tidak |

---

## Bagian C — Pertanyaan Diskusi Konseptual

Jawab pertanyaan berikut berdasarkan teori di modul dan pengamatan dari Latihan 1–5.

---

**C.1 — Pada Latihan 3, Query 1 menggunakan `outputMode("update")` sedangkan Query 2 menggunakan `outputMode("complete")`. Mengapa Query 2 tidak bisa menggunakan mode `update`? Apa yang terjadi jika Anda mencoba mengubahnya?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**C.2 — Ketika pipeline di-restart pada Latihan 4, Spark membaca kembali dari offset checkpoint. Jika producer terus mengirim event selama Spark berhenti, apakah event-event tersebut hilang? Jelaskan mekanisme apa yang memastikan event tidak hilang.**

> Tulis jawaban Anda di sini:
>
> _..._

---

**C.3 — Pada skrip producer (Latihan 2), terdapat simulasi out-of-order event. Apa yang terjadi jika event tersebut tiba setelah watermark sudah melewati window yang seharusnya? Apakah event akan dimasukkan ke hasil agregasi?**

> Petunjuk: Kaitkan dengan konsep event lag, watermark, dan mekanisme "tutup window".
> Tulis jawaban Anda di sini:
>
> _..._

---

**C.4 — Mengapa jumlah consumer dalam satu consumer group yang lebih besar dari jumlah partisi justru tidak meningkatkan throughput? Apa trade-off dalam memilih jumlah partisi topic Kafka?**

> Petunjuk: Aturan fundamental: satu partisi hanya bisa dibaca oleh satu consumer dalam satu group.
> Tulis jawaban Anda di sini:
>
> _..._

---

**C.5 — Bandingkan overhead checkpoint antara query dengan window aggregation (Query 1) dan query tanpa window (Query 2 — global groupBy). Mana yang lebih berat secara komputasi? Mengapa?**

> Petunjuk: Pikirkan tentang berapa banyak state yang harus disimpan di setiap kasus.
> Tulis jawaban Anda di sini:
>
> _..._

---

## Refleksi dan Analisis

**R5.1 — Dari Tabel A.3, sliding window menghasilkan lebih banyak baris output per trigger. Dalam konteks bisnis nyata (misalnya: memantau transaksi mencurigakan setiap 5 menit dengan data 10 menit terakhir), kapan sliding window lebih tepat digunakan dibandingkan tumbling window?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R5.2 — Dari Tabel B.1, apakah ada duplikat yang terdeteksi dalam 100 event pertama? Jika tidak ada duplikat, apakah itu berarti producer menggunakan exactly-once semantics? Jelaskan perbedaan antara "tidak ada duplikat yang terdeteksi" dan "exactly-once dijamin".**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R5.3 — Dari Tabel B.2, setelah run ke-1 selesai dengan `consumer.commit()`, run ke-2 tidak membaca event yang sama. Ini menunjukkan perilaku at-least-once atau at-most-once? Kapan duplikasi bisa terjadi pada konfigurasi ini?**

> Petunjuk: Pikirkan skenario: consumer crash setelah `consumer.commit()` tapi sebelum menyimpan hasil ke database.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R5.4 — Dari seluruh rangkaian latihan (1–5), gambarkan arsitektur pipeline yang telah Anda bangun dalam format teks sederhana. Sertakan: sumber data → broker → processing → sink. Tambahkan mekanisme fault tolerance di setiap lapisan.**

> Contoh format:
> ```
> [Producer Python] → (key-based routing) → [Kafka: 3 partisi]
>        ↓ (offset tracking)
> [Spark Structured Streaming]
>        ↓ (checkpointing ke /tmp/)
> [Console Sink]
> ```
> Gambarkan versi lengkap dengan komponen fault tolerance:
>
> _..._

---

**R5.5 — Refleksi akhir: Dari semua konsep yang dipelajari di modul ini (batch vs streaming, Kafka, partitioning, delivery semantics, windowing, fault tolerance), konsep mana yang menurut Anda paling kritis untuk dipahami sebelum membangun sistem data real-time di lingkungan produksi? Berikan alasan berdasarkan pengalaman praktikum ini.**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Tabel Rangkuman Seluruh Latihan

Isi tabel ini sebagai rangkuman komprehensif dari Latihan 1 sampai 5:

| Komponen | Konfigurasi yang Digunakan | Fungsi dalam Pipeline |
|---|---|---|
| Kafka Broker | KRaft, 1 node, port 9092 | _..._ |
| Topic `transaksi-stream` | 3 partisi, RF=1 | _..._ |
| Producer Python | `acks=all`, idempotent | _..._ |
| Message Key | `user_id` | _..._ |
| Spark Structured Streaming | local[2], micro-batch | _..._ |
| Tumbling Window | 1 menit (Latihan 3) | _..._ |
| Watermark | 2 menit | _..._ |
| Output Mode update | Query 1 | _..._ |
| Output Mode complete | Query 2 | _..._ |
| Checkpoint | `/tmp/checkpoints/` | _..._ |

---

## Kesimpulan Latihan 5

Setelah menyelesaikan seluruh rangkaian latihan Modul 8, lengkapi pernyataan berikut:

> "Sliding window menghasilkan **___** kali lebih banyak output dibandingkan tumbling window karena setiap event bisa masuk ke **___** window yang berbeda. Consumer dengan `enable_auto_commit=False` dan commit manual menerapkan semantics **___** (at-most-once / at-least-once / exactly-once), sehingga duplikasi bisa terjadi jika consumer **___** setelah proses tapi sebelum commit. Untuk menjamin exactly-once end-to-end, diperlukan kombinasi: idempotent producer + **___** di Spark + **___** write di sink."

---

## Penutup Modul 8

Selamat! Anda telah berhasil menyelesaikan seluruh rangkaian latihan Modul 8. Berikut ringkasan pencapaian:

| Latihan | Topik | Status |
|---|---|---|
| Latihan 1 | Setup lingkungan Kafka, verifikasi topic dan CLI | ☐ Selesai |
| Latihan 2 | Producer Python, key-based partitioning, format JSON | ☐ Selesai |
| Latihan 3 | Spark Streaming, tumbling window, dual query, Spark UI | ☐ Selesai |
| Latihan 4 | Fault tolerance, checkpoint, recovery tanpa kehilangan event | ☐ Selesai |
| Latihan 5 | Sliding vs tumbling, delivery semantics, diskusi konseptual | ☐ Selesai |

---

*Modul 8 — Analitik Aliran Data · Institut Teknologi Sumatera*