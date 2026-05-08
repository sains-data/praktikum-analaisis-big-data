# Keterangan File Data — Modul 8

## Daftar File

| File | Jumlah Record | Digunakan Di | Keterangan |
|---|---|---|---|
| `sample_events.json` | 10 | Latihan 1, 2 | Referensi schema dan contoh format event transaksi |
| `transaksi_historis.json` | 500 | Latihan 2, 3 | Data seed untuk topic `transaksi-stream` sebelum latihan dimulai. Mengandung ~20 out-of-order event untuk menguji watermark. |
| `sensor_iot_historis.json` | 300 | Latihan 5 (eksplorasi) | Data seed untuk topic `sensor-iot` |
| `transaksi_duplikat_test.json` | 50 (40 unik) | Latihan 5 Bagian B | 50 event dengan 10 event_id duplikat, didesain untuk menguji analisis delivery semantics |
| `referensi_schema.json` | — | Semua latihan | Dokumentasi lengkap schema semua topic Kafka |
| `seed_kafka.py` | — | Setup awal | Script Python untuk mengirim data historis ke Kafka sebelum latihan |

## Cara Menggunakan

### Seed topic `transaksi-stream` (untuk Latihan 2 & 3):
```bash
python data/seed_kafka.py
```

### Seed topic `sensor-iot` (untuk Latihan 5):
```bash
python data/seed_kafka.py \
  --topic sensor-iot \
  --file data/sensor_iot_historis.json
```

### Seed topic untuk uji delivery semantics (Latihan 5 Bagian B):
```bash
python data/seed_kafka.py \
  --topic transaksi-stream \
  --file data/transaksi_duplikat_test.json \
  --delay 0.0
```

## Catatan Penting

- Semua event_time menggunakan format **ISO 8601 UTC** (`+00:00`)
- `transaksi_historis.json` mengandung **~4% out-of-order event** (event_time lebih awal dari urutan kirim) untuk mensimulasikan late data
- `transaksi_duplikat_test.json` memiliki **10 dari 50 event_id yang duplikat** (acak, tersebar)
- Data di-generate dengan `random.seed(42)` sehingga **reproducible** — hasilnya selalu sama setiap generate ulang