"""
seed_kafka.py
Mengirim data historis ke Kafka sebelum latihan dimulai.
Gunakan ini untuk mengisi topic dengan data awal tanpa perlu
menunggu producer real-time berjalan lama.

Jalankan dari direktori modul8/:
  python data/seed_kafka.py
  python data/seed_kafka.py --topic sensor-iot --file data/sensor_iot_historis.json
"""

import json
import time
import argparse
import pathlib
from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"

def seed(topic: str, filepath: str, delay: float = 0.02):
    print(f"\n[Seeder] Topic   : {topic}")
    print(f"[Seeder] File    : {filepath}")
    print(f"[Seeder] Delay   : {delay}s antar event")

    with open(filepath) as f:
        events = json.load(f)

    print(f"[Seeder] Total   : {len(events)} event akan dikirim...\n")

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        enable_idempotence=True,
    )

    sent = 0
    for ev in events:
        # Gunakan user_id atau sensor_id sebagai key
        key = ev.get("user_id") or ev.get("sensor_id")
        producer.send(topic, key=key, value=ev)
        sent += 1
        if sent % 50 == 0:
            print(f"  [{sent:>4}/{len(events)}] Terkirim... event_id={ev.get('event_id')}")
        time.sleep(delay)

    producer.flush()
    producer.close()
    print(f"\n[Seeder] Selesai. Total terkirim: {sent} event ke topic '{topic}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed Kafka topic dengan data JSON.")
    parser.add_argument("--topic", default="transaksi-stream",
                        help="Nama topic Kafka (default: transaksi-stream)")
    parser.add_argument("--file",  default="data/transaksi_historis.json",
                        help="Path ke file JSON (default: data/transaksi_historis.json)")
    parser.add_argument("--delay", type=float, default=0.02,
                        help="Jeda antar event dalam detik (default: 0.02)")
    args = parser.parse_args()

    seed(args.topic, args.file, args.delay)
