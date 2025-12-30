import os
import time
import json
from confluent_kafka import Producer

BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPICS = [
    "chat-room-1",
    "chat-room-2",
    "chat-room-3",
    "chat-room-4",
]


conf = {
    "bootstrap.servers": BROKER,
    "linger.ms": 30,                 
    "batch.num.messages": 10000,     
    "queue.buffering.max.messages": 1000000,
    "queue.buffering.max.kbytes": 262144, 
    "compression.type": "lz4",
    "acks": 1,
    "queue.buffering.max.messages": 2000000,
    "queue.buffering.max.kbytes": 524288,  # 512 MB

}

producer = Producer(conf)

print("🚀 High-throughput producer started")

sent = 0
start = time.time()

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")

try:
    while True:
        payload = json.dumps({
            "id": sent,
            "msg": "x" * 500,
            "ts": time.time()
        }).encode()

        topic = TOPICS[sent % len(TOPICS)]
        while True:
            try:
                producer.produce(topic, value=payload)
                break
            except BufferError:
                producer.poll(0.01)

        sent += 1

        if sent % 5000 == 0:
            producer.poll(0)
            elapsed = time.time() - start
            print(f"Produced {sent} msgs | Rate: {int(sent/elapsed)} msg/sec")

except KeyboardInterrupt:
    pass

finally:
    producer.flush()
