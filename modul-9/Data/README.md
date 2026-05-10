# Keterangan File Data — Modul 9

Letakkan semua file ini di dalam folder `bigdata-spark/modul9/data/`.

## Daftar File

| File | Record | Format | Digunakan Di | Keterangan |
|---|---|---|---|---|
| `transaksi_ml.json`,`transaksi_ml.parquet` | 10.000 | JSON, Parquet | Tahap 1–5 | Dataset utama — seed ke HDFS sebelum latihan |
| `transaksi_ml.csv`, `transaksi_ml.parquet` | 10.000 | CSV, Parquet | Eksplorasi lokal | Format pandas-friendly, tanpa perlu Spark |
| `transaksi_ml_sample.json`, `transaksi_ml_sample.parquet` | 100 | JSON, Parquet | Referensi cepat | Cuplikan tiap 100 baris untuk melihat schema |
| `pelanggan_agregat.json`, `pelanggan_agregat.parquet` | 200 | JSON, Parquet | Tahap 3 preview | Agregasi per pelanggan — preview input K-Means |
| `referensi_schema.json`, `referensi_schema.parquet` | — | JSON, Parquet | Semua tahap | Dokumentasi schema, label, fitur, statistik |

## Cara Menggunakan

### Seed dataset ke HDFS sebelum latihan dimulai

```bash
# 1. Masuk ke kontainer
bash login.sh

# 2. Salin script generator
cp /modul9/scripts/buat_data_ml.py /tmp/

# 3. Jalankan generator (membuat data langsung di HDFS)
spark-submit \
  --master yarn --deploy-mode client \
  --executor-memory 512m --num-executors 2 \
  /tmp/buat_data_ml.py

# 4. Verifikasi
hdfs dfs -ls /datalake/silver/transaksi/
```

### Eksplorasi cepat tanpa Spark (dari laptop/PC)

```python
import json, pandas as pd

# Baca JSON
rows = json.load(open("data/transaksi_ml.json"))
print(f"Total baris: {len(rows)}")

# Baca CSV dengan pandas
df = pd.read_csv("data/transaksi_ml.csv")
print(df.describe())
print(df["segmen"].value_counts())
```

## Catatan Penting

- Dataset di-generate dengan `random.seed(42)` → **reproducible**, hasil selalu sama
- `total_nilai` dihitung dari `harga_satuan` presisi penuh (sebelum di-round ke 2 desimal)
  sehingga ada selisih kecil (<Rp1) jika dihitung ulang dari kolom `harga_satuan` yang
  tampil — ini **normal**, sama seperti sistem POS nyata
- Distribusi segmen **tidak seimbang**: tinggi (77.6%) >> menengah (21.1%) > rendah (1.3%)
  → relevan untuk diskusi Precision/Recall vs Accuracy di Tahap 2
- `pelanggan_agregat.json` hanya digunakan sebagai **preview lokal** K-Means;
  di dalam latihan, agregasi dilakukan oleh Spark dari `/datalake/silver/transaksi/`
