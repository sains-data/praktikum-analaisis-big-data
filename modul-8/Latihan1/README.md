# Latihan 1 — Persiapan dan Verifikasi Lingkungan
**Modul 8 · Analitik Aliran Data** | Estimasi waktu: **10 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Menjalankan dan memverifikasi lingkungan Kafka berbasis Docker
- Membuat dan menginspeksi topic Kafka beserta metadata partisinya
- Mengirim dan menerima pesan secara manual menggunakan Kafka CLI
- Mengamati latensi pengiriman pesan secara subjektif

---

## Prasyarat

Pastikan langkah-langkah berikut sudah selesai sebelum memulai:
- [ ] Docker Engine berjalan
- [ ] Folder `praktikum-bigdata/modul8/` sudah tersedia dengan seluruh file dari panduan setup
- [ ] Terminal/command prompt terbuka di direktori `modul8/`

---

## Langkah Kerja

### Langkah 1.1 — Jalankan lingkungan Docker

Buka terminal dan arahkan ke direktori `modul8/`, lalu jalankan:

```bash
docker compose up -d
```

Tunggu sekitar 30–60 detik, kemudian cek status container:

```bash
docker compose ps
```

**Output yang diharapkan:**

```
NAME           IMAGE                          STATUS
kafka-broker   apache/kafka:3.7.0             Up (healthy)
kafka-ui       provectuslabs/kafka-ui:latest  Up
```

> Jika status masih `starting`, tunggu 15 detik lagi lalu ulangi perintah `docker compose ps`.

---

### Langkah 1.2 — Buat topic latihan

Jalankan script inisialisasi topic:

```bash
bash scripts/init_topics.sh
```

Setelah selesai, verifikasi topic berhasil dibuat:

```bash
docker exec kafka-broker kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

**Output yang diharapkan:**

```
penjualan-agregat
sensor-iot
transaksi-stream
```

---

### Langkah 1.3 — Amati metadata topic

Jalankan perintah berikut untuk melihat detail konfigurasi topic `transaksi-stream`:

```bash
docker exec kafka-broker kafka-topics.sh \
  --describe \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream
```

**Contoh output:**

```
Topic: transaksi-stream   TopicId: XYZ123   PartitionCount: 3   ReplicationFactor: 1
  Topic: transaksi-stream   Partition: 0   Leader: 1   Replicas: 1   Isr: 1
  Topic: transaksi-stream   Partition: 1   Leader: 1   Replicas: 1   Isr: 1
  Topic: transaksi-stream   Partition: 2   Leader: 1   Replicas: 1   Isr: 1
```

**Catat hasil berikut pada tabel pengamatan di bawah.**

---

### Langkah 1.4 — Uji pengiriman dan penerimaan pesan manual

Buka **dua terminal secara bersamaan**.

**Terminal A — Consumer (mendengarkan pesan):**

```bash
docker exec -it kafka-broker kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream \
  --from-beginning
```

**Terminal B — Producer (mengirim pesan):**

```bash
docker exec -it kafka-broker kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic transaksi-stream
```

Ketik **5 baris teks berbeda** di Terminal B (tekan Enter setelah setiap baris), contoh:

```
pesan pertama dari producer
transaksi senilai 500000
user-001 membeli elektronik
test latency pengiriman
pesan terakhir nomor lima
```

Amati apakah setiap pesan **langsung muncul** di Terminal A setelah dikirim dari Terminal B.

Hentikan kedua terminal dengan `Ctrl+C` setelah selesai mengamati.

---

### Langkah 1.5 — Verifikasi melalui Kafka UI

Buka browser dan akses:

```
http://localhost:8080
```

Navigasi ke: **Topics → transaksi-stream → Messages**

Verifikasi bahwa 5 pesan yang dikirim di Langkah 1.4 muncul di sana.

---

## Tabel Pencatatan Hasil

Isi tabel berikut berdasarkan output dari setiap langkah di atas.

### Tabel 1.1 — Metadata Topic `transaksi-stream`

| Informasi | Nilai yang Tercatat |
|---|---|
| Jumlah partisi | _(isi dari output `--describe`)_ |
| Replication factor | _(isi dari output `--describe`)_ |
| Leader broker untuk Partisi 0 (ID) | _(isi dari output `--describe`)_ |
| ISR (In-Sync Replicas) untuk Partisi 0 | _(isi dari output `--describe`)_ |
| Leader broker untuk Partisi 1 (ID) | _(isi dari output `--describe`)_ |
| Leader broker untuk Partisi 2 (ID) | _(isi dari output `--describe`)_ |

### Tabel 1.2 — Pengamatan Uji Pesan Manual

| Aspek Pengamatan | Catatan |
|---|---|
| Apakah pesan muncul di Terminal A setelah dikirim? | Ya / Tidak |
| Estimasi latensi yang dirasakan (subjektif) | _contoh: < 1 detik / beberapa detik_ |
| Apakah urutan pesan di Terminal A sama dengan urutan pengiriman? | Ya / Tidak |
| Apakah pesan terlihat di Kafka UI? | Ya / Tidak |
| Offset pesan pertama yang terlihat di Kafka UI | _(catat nomor offset)_ |

---

## Refleksi dan Analisis

Jawab pertanyaan berikut berdasarkan hasil pengamatan Anda.

**R1.1 — Mengapa semua partisi memiliki Leader dengan ID broker yang sama?**

> Petunjuk: Ingat bahwa lingkungan lab ini hanya menggunakan satu broker (single-node).
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.2 — Pada Tabel 1.1, nilai ISR untuk Partisi 0 adalah `1`. Apa artinya angka tersebut, dan apa hubungannya dengan Replication Factor yang juga bernilai 1?**

> Petunjuk: ISR (In-Sync Replica) adalah daftar broker yang menyimpan salinan data yang sudah sinkron dengan leader.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.3 — Pesan yang Anda kirim di Langkah 1.4 adalah teks biasa (bukan JSON). Jika Anda membuka tab `Messages` di Kafka UI, apakah Kafka membedakan format isi pesan? Apa implikasinya untuk aplikasi nyata?**

> Petunjuk: Kafka adalah platform agnostik format — ia hanya menyimpan bytes.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.4 — Anda menggunakan flag `--from-beginning` saat menjalankan consumer di Terminal A. Apa yang terjadi jika flag tersebut dihapus? Kapan penggunaan `--from-beginning` diperlukan dan kapan tidak?**

> Petunjuk: Pikirkan tentang offset dan kapan consumer pertama kali bergabung ke topic.
> Tulis jawaban Anda di sini:
>
> _..._

---

**R1.5 — Bandingkan ISR = 1 pada lingkungan lab ini dengan sistem produksi yang umum menggunakan `min.insync.replicas=2`. Apa risiko yang ada di lingkungan lab ini, dan mengapa konfigurasi tersebut bisa diterima untuk keperluan pembelajaran?**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 1

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Kafka broker berjalan dalam mode **___** (KRaft/ZooKeeper). Topic `transaksi-stream` memiliki **___** partisi dengan replication factor **___**. Pesan yang dikirim producer langsung diterima consumer dengan latensi yang dirasakan **___**. Ini menunjukkan bahwa Kafka mendukung paradigma **___** (push/pull) di mana consumer secara aktif melakukan polling ke broker."

---

*Latihan 1 selesai. Lanjutkan ke **Latihan 2 — Producer Python: Simulasi Event Transaksi**.*