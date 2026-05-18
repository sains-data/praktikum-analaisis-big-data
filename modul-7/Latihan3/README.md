# Latihan 3 — Menjelajahi Apache Atlas
**Modul 7 · Orkestrasi Alur Kerja dan Tata Kelola Big Data** | Estimasi waktu: **20 menit**

---

## Tujuan Latihan

Setelah menyelesaikan latihan ini, mahasiswa mampu:
- Mendaftarkan entitas tabel Hive (Bronze dan Silver) ke Apache Atlas via REST API
- Menambahkan klasifikasi sensitivitas data (`PII`, `FINANSIAL`) pada entitas dan kolom
- Mengambil dan membaca informasi lineage dari Atlas untuk menelusuri asal-usul data
- Menggunakan Atlas Web UI untuk mencari entitas, melihat detail, dan memvisualisasikan lineage
- Memahami konsep propagasi klasifikasi (classification propagation)

---

## Prasyarat

- [ ] Latihan 1 dan 2 selesai
- [ ] Atlas berjalan (`curl -u admin:admin http://localhost:21000/api/atlas/admin/status` → HTTP 200)
- [ ] Data Silver tersedia di HDFS (`hdfs dfs -ls /datalake/silver/latihan/`)
- [ ] Library `requests` tersedia (`pip show requests`)

---

## Langkah Kerja

### Langkah 3.1 — Verifikasi koneksi ke Atlas REST API

```bash
source ~/airflow-env/bin/activate

# Cek status Atlas server
curl -s -u admin:admin \
  http://localhost:21000/api/atlas/admin/status \
  | python3 -m json.tool

# Cek tipe entitas yang tersedia (Hive-related)
curl -s -u admin:admin \
  "http://localhost:21000/api/atlas/v2/types/typedefs" \
  | python3 -c "
import json, sys
data = json.load(sys.stdin)
entity_defs = data.get('entityDefs', [])
hive_types = [e['name'] for e in entity_defs if 'hive' in e['name']]
print('Tipe Hive yang tersedia:')
for t in sorted(hive_types): print(f'  {t}')
print(f'Total tipe Hive: {len(hive_types)}')
"
```

Catat tipe Hive yang tersedia pada **Tabel 3.1**.

---

### Langkah 3.2 — Buat dan jalankan script pendaftaran entitas

```bash
nano /tmp/daftar_entitas.py
```

Salin kode berikut:

```python
import requests
import json

BASE = "http://localhost:21000/api/atlas/v2"
AUTH = ("admin", "admin")
HDR  = {"Content-Type": "application/json"}


def daftar_tabel(nama, db, deskripsi, kolom_list):
    """Mendaftarkan satu entitas tabel Hive beserta kolomnya ke Atlas."""
    kolom_entities = []
    for col in kolom_list:
        kolom_entities.append({
            "typeName": "hive_column",
            "attributes": {
                "name": col["name"],
                "type": col["type"],
                "qualifiedName": f"{db}.{nama}.{col['name']}@cluster1",
                "table": {
                    "typeName": "hive_table",
                    "uniqueAttributes": {
                        "qualifiedName": f"{db}.{nama}@cluster1"
                    }
                }
            }
        })

    payload = {
        "entities": [
            {
                "typeName": "hive_table",
                "attributes": {
                    "name": nama,
                    "qualifiedName": f"{db}.{nama}@cluster1",
                    "db": {
                        "typeName": "hive_db",
                        "uniqueAttributes": {
                            "qualifiedName": f"{db}@cluster1"
                        }
                    },
                    "description": deskripsi,
                    "owner": "mahasiswa",
                    "tableType": "MANAGED_TABLE",
                    "temporary": False,
                }
            }
        ] + kolom_entities
    }

    resp = requests.post(
        f"{BASE}/entity/bulk",
        auth=AUTH, headers=HDR,
        data=json.dumps(payload)
    )
    result = resp.json()
    print(f"\n[DAFTAR] Tabel '{nama}' → HTTP {resp.status_code}")
    guids = result.get("guidAssignments", {})
    print(f"  GUID yang diberikan ({len(guids)}):")
    for k, v in list(guids.items())[:3]:
        print(f"    ...{k[-8:]} → {v}")
    return guids


# ── Daftarkan tabel Bronze ──────────────────────────────────────
daftar_tabel(
    nama="transaksi_bronze",
    db="datalake",
    deskripsi="Tabel raw data transaksi harian (Bronze layer) — data mentah dari sumber CSV",
    kolom_list=[
        {"name": "id",       "type": "string"},
        {"name": "nilai",    "type": "string"},
        {"name": "kategori", "type": "string"},
    ]
)

# ── Daftarkan tabel Silver ──────────────────────────────────────
daftar_tabel(
    nama="transaksi_silver",
    db="datalake",
    deskripsi="Tabel transaksi yang sudah dibersihkan (Silver layer) — hasil Spark ETL",
    kolom_list=[
        {"name": "id",             "type": "string"},
        {"name": "nilai",          "type": "double"},
        {"name": "kategori",       "type": "string"},
        {"name": "tanggal_proses", "type": "string"},
    ]
)

print("\n[OK] Pendaftaran entitas selesai.")
```

Jalankan script:

```bash
python3 /tmp/daftar_entitas.py
```

Catat GUID yang diberikan Atlas pada **Tabel 3.2**.

---

### Langkah 3.3 — Tambahkan klasifikasi sensitivitas data

```bash
nano /tmp/klasifikasi_pii.py
```

Salin kode berikut:

```python
import requests
import json

BASE = "http://localhost:21000/api/atlas/v2"
AUTH = ("admin", "admin")
HDR  = {"Content-Type": "application/json"}


def cari_guid(nama_tabel, nama_kolom=None):
    """Mencari GUID entitas tabel atau kolom di Atlas."""
    if nama_kolom:
        qn   = f"datalake.{nama_tabel}.{nama_kolom}@cluster1"
        tipe = "hive_column"
    else:
        qn   = f"datalake.{nama_tabel}@cluster1"
        tipe = "hive_table"

    resp = requests.get(
        f"{BASE}/entity/uniqueAttribute/type/{tipe}",
        auth=AUTH,
        params={"attr:qualifiedName": qn}
    )
    if resp.status_code != 200:
        print(f"  [TIDAK DITEMUKAN] {qn} (HTTP {resp.status_code})")
        return None
    guid = resp.json()["entity"]["guid"]
    print(f"  [DITEMUKAN] {qn}")
    print(f"              GUID: {guid}")
    return guid


def tambah_klasifikasi(guid, nama_klasifikasi, propagate=True):
    """Menambahkan klasifikasi/tag pada entitas."""
    payload = [{
        "typeName": nama_klasifikasi,
        "propagate": propagate,
        "removePropagationsOnEntityDelete": True,
    }]
    resp = requests.post(
        f"{BASE}/entity/guid/{guid}/classifications",
        auth=AUTH, headers=HDR,
        data=json.dumps(payload)
    )
    status = "OK" if resp.status_code in (200, 204) else f"GAGAL ({resp.status_code})"
    print(f"  Klasifikasi [{nama_klasifikasi}] → {status}")
    return resp.status_code


# ── Tambah klasifikasi PII ke kolom 'id' di Bronze ─────────────
print("\n=== Mencari kolom 'id' di tabel Bronze ===")
guid_id_bronze = cari_guid("transaksi_bronze", "id")
if guid_id_bronze:
    tambah_klasifikasi(guid_id_bronze, "PII", propagate=True)

# ── Tambah klasifikasi PII ke kolom 'id' di Silver ─────────────
print("\n=== Mencari kolom 'id' di tabel Silver ===")
guid_id_silver = cari_guid("transaksi_silver", "id")
if guid_id_silver:
    tambah_klasifikasi(guid_id_silver, "PII", propagate=True)

# ── Tambah klasifikasi FINANSIAL ke tabel Silver ────────────────
print("\n=== Mencari tabel Silver ===")
guid_silver = cari_guid("transaksi_silver")
if guid_silver:
    tambah_klasifikasi(guid_silver, "FINANSIAL", propagate=False)

# ── Tambah klasifikasi SENSITIF ke kolom 'nilai' di Silver ──────
print("\n=== Mencari kolom 'nilai' di Silver ===")
guid_nilai_silver = cari_guid("transaksi_silver", "nilai")
if guid_nilai_silver:
    tambah_klasifikasi(guid_nilai_silver, "SENSITIF", propagate=True)

print("\n[OK] Semua klasifikasi ditambahkan.")
```

Jalankan:

```bash
python3 /tmp/klasifikasi_pii.py
```

Catat hasil HTTP status setiap operasi pada **Tabel 3.2**.

---

### Langkah 3.4 — Buat dan jalankan script lineage

```bash
nano /tmp/ambil_lineage.py
```

Salin kode berikut:

```python
import requests
import json

BASE = "http://localhost:21000/api/atlas/v2"
AUTH = ("admin", "admin")


def cari_guid_tabel(nama_tabel):
    resp = requests.get(
        f"{BASE}/entity/uniqueAttribute/type/hive_table",
        auth=AUTH,
        params={"attr:qualifiedName": f"datalake.{nama_tabel}@cluster1"}
    )
    if resp.status_code != 200:
        return None
    return resp.json()["entity"]["guid"]


def tampilkan_lineage(nama_tabel, direction="BOTH"):
    """Mengambil dan menampilkan lineage dari entitas tabel."""
    guid = cari_guid_tabel(nama_tabel)
    if not guid:
        print(f"[ERROR] Entitas tidak ditemukan: {nama_tabel}")
        return

    resp = requests.get(
        f"{BASE}/lineage/{guid}",
        auth=AUTH,
        params={"direction": direction, "depth": 5}
    )
    data = resp.json()

    entitas = data.get("guidEntityMap", {})
    relasi  = data.get("relations", [])

    print(f"\n{'='*55}")
    print(f" Lineage untuk: {nama_tabel} (direction={direction})")
    print(f"{'='*55}")
    print(f"Jumlah entitas terkait: {len(entitas)}")
    print(f"Jumlah relasi         : {len(relasi)}")

    if entitas:
        print("\nEntitas dalam lineage graph:")
        for g, info in entitas.items():
            tipe = info.get("typeName", "?")
            nama = info.get("attributes", {}).get("name", "?")
            marker = "  ◄─ TABEL INI" if g == guid else ""
            print(f"  [{tipe:20s}] {nama}{marker}")

    if relasi:
        print("\nRelasi (arah aliran data):")
        for rel in relasi:
            frm_id = rel.get("fromEntityId", "")
            to_id  = rel.get("toEntityId", "")
            frm_nama = entitas.get(frm_id, {}).get(
                "attributes", {}
            ).get("name", frm_id[:8] + "...")
            to_nama = entitas.get(to_id, {}).get(
                "attributes", {}
            ).get("name", to_id[:8] + "...")
            print(f"  {frm_nama:25s} ──► {to_nama}")
    else:
        print("\n[INFO] Tidak ada relasi lineage yang tercatat.")
        print("       Lineage akan terisi setelah Hive Hook aktif")
        print("       atau setelah DAG Tahap 4 dijalankan.")


# Tampilkan lineage dari perspektif tabel Silver
tampilkan_lineage("transaksi_silver", direction="BOTH")

# Tampilkan lineage dari perspektif tabel Bronze
tampilkan_lineage("transaksi_bronze", direction="OUTPUT")
```

Jalankan:

```bash
python3 /tmp/ambil_lineage.py
```

Catat semua output pada **Tabel 3.3**.

---

### Langkah 3.5 — Verifikasi melalui Atlas Web UI

Buka browser ke `http://localhost:21000`. Login dengan `admin / admin`.

**Pencarian tabel Bronze:**
1. Klik menu **Search** di navbar atas
2. Di dropdown **Entity Type**, pilih `hive_table`
3. Klik tombol **Search**
4. Klik nama `transaksi_bronze` dari hasil pencarian
5. Amati tab **Properties** — verifikasi atribut `description`, `owner`, `qualifiedName`
6. Klik tab **Classifications** — apakah ada klasifikasi yang tercantum?
7. Klik tab **Lineage** — apakah ada relasi yang tervisualisasi?

**Pencarian tabel Silver:**
1. Kembali ke Search, cari `transaksi_silver`
2. Klik tab **Properties** — verifikasi 4 kolom terdaftar
3. Klik tab **Classifications** — apakah klasifikasi `FINANSIAL` terlihat?
4. Klik tab **Lineage** — amati visualisasi lineage

Catat semua pengamatan pada **Tabel 3.4**.

---

### Langkah 3.6 — Cari entitas berdasarkan klasifikasi

```bash
python3 << 'EOF'
import requests, json

BASE = "http://localhost:21000/api/atlas/v2"
AUTH = ("admin", "admin")

# Cari semua entitas dengan klasifikasi PII
resp = requests.get(
    f"{BASE}/search/basic",
    auth=AUTH,
    params={
        "classification": "PII",
        "typeName": "hive_column",
        "limit": 20,
    }
)
results = resp.json()
print(f"\nEntitas bertipe hive_column dengan klasifikasi PII:")
print(f"Total: {results.get('count', 0)}")
for ent in results.get("entities", []):
    nama   = ent.get("attributes", {}).get("name", "?")
    tabel  = ent.get("attributes", {}).get(
        "qualifiedName", "?"
    ).split(".")[1] if "." in ent.get(
        "attributes", {}
    ).get("qualifiedName", "") else "?"
    print(f"  kolom '{nama}' di tabel '{tabel}'")

# Cari semua entitas dengan klasifikasi FINANSIAL
resp2 = requests.get(
    f"{BASE}/search/basic",
    auth=AUTH,
    params={
        "classification": "FINANSIAL",
        "limit": 20,
    }
)
results2 = resp2.json()
print(f"\nEntitas dengan klasifikasi FINANSIAL:")
print(f"Total: {results2.get('count', 0)}")
for ent in results2.get("entities", []):
    nama = ent.get("attributes", {}).get("name", "?")
    tipe = ent.get("typeName", "?")
    print(f"  [{tipe}] {nama}")
EOF
```

Catat pada **Tabel 3.3**.

---

## Tabel Pencatatan Hasil

### Tabel 3.1 — Tipe Entitas Hive yang Tersedia di Atlas

| Tipe Entitas | Ada? | Keterangan |
|---|---|---|
| `hive_table` | Ya / Tidak | Representasi tabel Hive |
| `hive_column` | Ya / Tidak | Representasi kolom |
| `hive_db` | Ya / Tidak | Representasi database Hive |
| `hive_process` | Ya / Tidak | Representasi proses/query yang membuat tabel |
| `spark_process` | Ya / Tidak | Representasi Spark job |
| **Total tipe Hive** | _..._ | — |

### Tabel 3.2 — Hasil Pendaftaran Entitas dan Klasifikasi

**Pendaftaran entitas:**

| Entitas | HTTP Status | Jumlah GUID Diberikan | Keterangan |
|---|---|---|---|
| `transaksi_bronze` (tabel + 3 kolom) | _..._ | _..._ | _..._ |
| `transaksi_silver` (tabel + 4 kolom) | _..._ | _..._ | _..._ |

**Klasifikasi yang ditambahkan:**

| Entitas | Kolom | Klasifikasi | HTTP Status | Propagate |
|---|---|---|---|---|
| transaksi_bronze | id | PII | _..._ | True |
| transaksi_silver | id | PII | _..._ | True |
| transaksi_silver | (tabel) | FINANSIAL | _..._ | False |
| transaksi_silver | nilai | SENSITIF | _..._ | True |

### Tabel 3.3 — Hasil Lineage dan Pencarian Klasifikasi

**Output script lineage (`ambil_lineage.py`):**

| Perspektif | Jumlah Entitas Terkait | Jumlah Relasi | Entitas Upstream | Entitas Downstream |
|---|---|---|---|---|
| `transaksi_silver` (BOTH) | _..._ | _..._ | _..._ | _..._ |
| `transaksi_bronze` (OUTPUT) | _..._ | _..._ | — | _..._ |

**Catatan lineage:**

```
(salin output relasi dari script di sini)
```

**Pencarian berdasarkan klasifikasi:**

| Klasifikasi | Tipe Dicari | Jumlah Entitas Ditemukan | Nama-nama Entitas |
|---|---|---|---|
| PII | hive_column | _..._ | _..._ |
| FINANSIAL | (semua) | _..._ | _..._ |

### Tabel 3.4 — Pengamatan Atlas Web UI

| Aspek yang Diamati | `transaksi_bronze` | `transaksi_silver` |
|---|---|---|
| Tab Properties — `qualifiedName` | _..._ | _..._ |
| Tab Properties — `owner` | _..._ | _..._ |
| Tab Properties — jumlah kolom terdaftar | _..._ | _..._ |
| Tab Classifications — klasifikasi yang terlihat | _..._ | _..._ |
| Tab Lineage — ada relasi? | Ya / Tidak | Ya / Tidak |
| Tab Lineage — deskripsi visual | _..._ | _..._ |

---

## Refleksi dan Analisis

**R3.1 — Dari Tabel 3.3, apakah lineage antara `transaksi_bronze` dan `transaksi_silver` sudah terbentuk otomatis? Jika belum ada relasi lineage, jelaskan mengapa — komponen apa yang belum aktif dalam lingkungan lab ini yang seharusnya mencatat lineage secara otomatis?**

> Petunjuk: Hive Hook adalah komponen yang mencatat lineage saat query HiveQL dieksekusi. Lineage via REST API (seperti yang akan dilakukan di Latihan 4) harus dibuat secara manual menggunakan entitas `spark_process` atau `hive_process`.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.2 — Klasifikasi `PII` ditambahkan dengan `propagate=True`, sedangkan `FINANSIAL` dengan `propagate=False`. Dalam konteks tata kelola data nyata, jelaskan perbedaan perilaku kedua konfigurasi ini. Berikan skenario di mana propagasi otomatis bisa berbahaya (false positive) dan skenario di mana propagasi sangat membantu.**

> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.3 — Dari Tabel 3.2, pendaftaran tabel Bronze menghasilkan beberapa GUID — satu untuk tabel dan satu untuk setiap kolom. Mengapa entitas kolom perlu didaftarkan secara terpisah (bukan hanya tabelnya)? Berikan satu use case konkret di mana pendetailan di level kolom sangat penting dalam audit kepatuhan regulasi.**

> Petunjuk: Pikirkan tentang GDPR — regulator tidak hanya ingin tahu "tabel mana yang menyimpan data personal", tetapi "kolom spesifik mana" yang berisi nama, alamat, atau nomor identitas.
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.4 — Dari Tabel 3.4, Atlas menyimpan metadata dalam format graf (BerkeleyDB sebagai backend). Mengapa representasi graf lebih tepat untuk metadata tata kelola data dibandingkan database relasional biasa (seperti PostgreSQL)?**

> Petunjuk: Bayangkan lineage yang dalam — Bronze → Silver → Gold → Dashboard BI → Laporan Eksekutif. Bagaimana relasi seperti ini direpresentasikan dalam tabel relasional vs graf?
>
> Tulis jawaban Anda di sini:
>
> _..._

---

**R3.5 — REST API Atlas mengembalikan HTTP 200 atau 204 untuk penambahan klasifikasi yang berhasil. Jika Anda mendapatkan HTTP 404 saat mencari entitas dengan `cari_guid()`, apa yang paling mungkin menjadi penyebabnya? Sebutkan tiga kemungkinan penyebab dan cara memverifikasi masing-masing.**

> Tulis jawaban Anda di sini:
>
> _..._

---

## Kesimpulan Latihan 3

Setelah menyelesaikan latihan ini, lengkapi pernyataan berikut:

> "Dua entitas tabel berhasil didaftarkan ke Apache Atlas: `transaksi_bronze` dengan **___** kolom dan `transaksi_silver` dengan **___** kolom. Klasifikasi yang diterapkan meliputi: **___** (pada kolom `id` Bronze dan Silver) dengan propagasi **___**, dan **___** (pada tabel Silver) tanpa propagasi. Lineage antara kedua tabel saat ini **___** (ada/belum ada) karena **___**. Perbedaan mendasar antara `propagate=True` dan `propagate=False` adalah: klasifikasi dengan propagasi akan **___** secara otomatis ke semua entitas yang **___** dari entitas tersebut."

---

*Latihan 3 selesai. Lanjutkan ke **Latihan 4 — Pipeline End-to-End Terintegrasi**.*