# Latihan 2 — Producer Python: Simulasi Event Transaksi
**Modul 8 · Analitik Aliran Data** | Estimasi waktu: **20 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Menulis dan menjalankan producer Python menggunakan library `kafka-python`
- Memahami peran message key dalam penentuan partisi
- Mengamati distribusi pesan ke partisi-partisi Kafka
- Memverifikasi format payload JSON event yang diterima Kafka
- Mengidentifikasi konfigurasi producer untuk delivery semantics yang kuat

---

## Prasyarat

- [ ] Latihan 1 sudah selesai dan lingkungan Docker berjalan
- [ ] `docker compose ps` menampilkan `kafka-broker` dengan status `Up (healthy)`
- [ ] Library Python sudah terpasang: `pip install -r requirements.txt`

---

## Langkah Kerja

### Langkah 2.1 — Tinjau kode producer sebelum dijalankan

Buka dan baca file `scripts/producer_transaksi.py`. Perhatikan bagian-bagian berikut sebelum menjalankannya:

```python
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks="all",                  # ← semua ISR harus konfirmasi
    enable_idempotence=True,     # ← cegah duplikat akibat retry
    retries=3,
)
```

Dan perhatikan fungsi `buat_event()` yang menentukan struktur setiap event:

```python
return {
    "event_id":   str(uuid.uuid4())[:8],   # ← ID unik 8 karakter
    "user_id":    user_id,                  # ← juga digunakan sebagai key
    "product":    random.choice(PRODUCTS),
    "channel":    random.choice(CHANNELS),
    "amount":     round(random.uniform(10_000, 5_000_000), 2),
    "event_time": event_time.isoformat(),
}, user_id  # ← user_id dikirim sebagai message key
```

> **Mengapa `user_id` dijadikan key?** Kafka menggunakan hash dari key untuk menentukan partisi tujuan. Semua transaksi dari user yang sama akan selalu masuk ke partisi yang sama, menjamin urutan pemrosesan per pengguna.

---

### Langkah 2.2 — Jalankan producer

Buka terminal baru (Terminal 1) dan jalankan:

```bash
python scripts/producer_transaksi.py
```

Biarkan producer berjalan. Output yang muncul setiap 10 event:

```
[Producer] Mengirim event ke 'transaksi-stream'... (Ctrl+C untuk berhenti)
  [  10 event terkirim] id=3a9f21b0 | user=usr-0023 | channel=web | amount=   872,345
  [  20 event terkirim] id=7c1e44f2 | user=usr-0007 | channel=mobile | amount= 1,204,000
  ...
```

Catat waktu mulai producer pada **Tabel 2.1**.

---

### Langkah 2.3 — Verifikasi event masuk ke Kafka via CLI

Buka terminal baru (Terminal 2) dan jalankan:

```bash
docker exec kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream \
  --from-beginning \
  --max-messages 5
```

Amati **5 event pertama** yang muncul. Perhatikan format JSON-nya.

---

### Langkah 2.4 — Amati distribusi partisi via Kafka UI

Buka browser ke `http://localhost:8080`, navigasi ke:

**Topics → transaksi-stream → Overview**

Amati grafik **Messages per Partition**. Setelah producer berjalan ~2 menit, catat jumlah pesan di setiap partisi.

Kemudian navigasi ke:

**Topics → transaksi-stream → Messages**

Klik beberapa pesan dan amati field **Partition** dan **Offset** di detail pesan.

---

### Langkah 2.5 — Cek distribusi offset per partisi via CLI

Jalankan perintah berikut untuk melihat jumlah pesan yang tersimpan di setiap partisi:

```bash
docker exec kafka-broker kafka-run-class.sh kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream
```

**Contoh output:**

```
transaksi-stream:0:87
transaksi-stream:1:84
transaksi-stream:2:89
```

Format: `topic:partisi:jumlah-offset`

Catat angka-angka ini pada **Tabel 2.2**. Ulangi perintah ini setelah 1 menit untuk melihat pertambahannya.

---

### Langkah 2.6 — Verifikasi key-based partitioning

Pilih satu `user_id` dari output producer, misalnya `usr-0015`. Kemudian filter pesan dari user tersebut di Kafka UI:

**Topics → transaksi-stream → Messages → Filter by key: `usr-0015`**

Catat nomor partisi dari **semua pesan** dengan key tersebut. Apakah semua masuk ke partisi yang sama?

---

### Langkah 2.7 — Hentikan producer setelah ~3 menit

Kembali ke Terminal 1 dan tekan `Ctrl+C`. Perhatikan pesan shutdown:

```
[Producer] Dihentikan. Total terkirim: 342
```

Catat total event yang terkirim pada **Tabel 2.1**.

---

## Tabel Pencatatan Hasil

### Tabel 2.1 — Ringkasan Pengiriman Event

| Informasi | Nilai yang Tercatat |
|---|---|
| Waktu mulai producer | _HH:MM:SS_ |
| Waktu selesai producer | _HH:MM:SS_ |
| Total durasi berjalan | _menit:detik_ |
| Total event terkirim | _(dari pesan shutdown)_ |
| Rata-rata event per detik | _(hitung: total / durasi)_ |

### Tabel 2.2 — Distribusi Event per Partisi

| Partisi | Offset Awal (T=0) | Offset setelah 1 menit (T=1) | Pertambahan |
|---|---|---|---|
| Partisi 0 | _..._ | _..._ | _..._ |
| Partisi 1 | _..._ | _..._ | _..._ |
| Partisi 2 | _..._ | _..._ | _..._ |
| **Total** | _..._ | _..._ | _..._ |

### Tabel 2.3 — Verifikasi Format Event

Salin satu event JSON dari output CLI (Langkah 2.3) dan tempelkan di bawah:

```json
(tempel event JSON di sini)
```

Kemudian isi tabel validasi berikut:

| Field | Ada? | Tipe Data | Contoh Nilai |
|---|---|---|---|
| `event_id` | Ya/Tidak | _..._ | _..._ |
| `user_id` | Ya/Tidak | _..._ | _..._ |
| `product` | Ya/Tidak | _..._ | _..._ |
| `channel` | Ya/Tidak | _..._ | _..._ |
| `amount` | Ya/Tidak | _..._ | _..._ |
| `event_time` | Ya/Tidak | _..._ | _..._ |

### Tabel 2.4 — Verifikasi Key-Based Partitioning

Pilih satu `user_id` dan catat partisi dari minimal 3 pesannya:

| user_id yang dipilih | Pesan ke- | Offset | Partisi |
|---|---|---|---|
| _usr-xxxx_ | 1 | _..._ | _..._ |
| _usr-xxxx_ | 2 | _..._ | _..._ |
| _usr-xxxx_ | 3 | _..._ | _..._ |
| Apakah semua di partisi yang sama? | **Ya / Tidak** | — | — |

---

## Refleksi dan Analisis

**R2.1 — Dari Tabel 2.2, apakah distribusi pesan antar partisi merata? Mengapa distribusinya bisa tidak persis sama meskipun menggunakan key-based partitioning?**

> Petunjuk: Pikirkan tentang distribusi `user_id` yang dihasilkan `random.choice()` dan fungsi hash Kafka.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.2 — Producer dikonfigurasi dengan `acks="all"` dan `enable_idempotence=True`. Jelaskan apa yang terjadi secara internal jika jaringan antara producer dan broker tiba-tiba terputus sesaat setelah pesan dikirim tetapi sebelum konfirmasi diterima.**

> Petunjuk: Hubungkan dengan konsep delivery semantics (at-least-once vs exactly-once) yang dijelaskan di modul.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.3 — Pada Tabel 2.4, Anda membuktikan bahwa semua pesan dari user yang sama selalu masuk ke partisi yang sama. Apa keuntungan dan kerugian dari strategi ini dibandingkan round-robin (tanpa key)?**

> Petunjuk: Pikirkan tentang urutan pemrosesan, paralelisme, dan distribusi beban.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.4 — Format `event_time` yang dihasilkan producer menggunakan `.isoformat()` dari Python, menghasilkan string seperti `"2024-04-15T10:23:45.123456+00:00"`. Mengapa format timestamp ini penting dalam sistem streaming? Apa yang terjadi jika dua event dari sumber berbeda menggunakan timezone yang berbeda?**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R2.5 — Kode producer menyertakan simulasi out-of-order event dengan komentar `# Simulasi: 5% event terlambat`. Dalam kode asli di modul, ada variabel `delay` dengan nilai negatif yang ditambahkan ke `event_time`. Jelaskan apa yang akan terjadi pada event ini saat diproses oleh Spark Structured Streaming dengan watermark. Apakah event akan selalu diproses?**

> Petunjuk: Baca kembali subbab 2.7 tentang Watermark dan Late Data.
> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 2

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Producer Python mengirim event ke topic `transaksi-stream` dengan rata-rata **___** event/detik. Distribusi ke **___** partisi dilakukan berdasarkan **___** (hash key / round-robin). Terbukti bahwa semua event dari user yang sama masuk ke partisi **___** (berbeda/sama). Konfigurasi `enable_idempotence=True` memastikan delivery semantics **___** (at-most-once / at-least-once / exactly-once) di sisi producer."

---

*Latihan 2 selesai. Lanjutkan ke **Latihan 3 — Spark Structured Streaming: Agregasi Window**.*