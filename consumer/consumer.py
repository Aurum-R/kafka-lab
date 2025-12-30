import os
import time
from confluent_kafka import Consumer

BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC = os.getenv("TOPIC_NAME", "chat-room-1")
GROUP = os.getenv("GROUP_ID", "room-1-subscribers")

conf = {
    "bootstrap.servers": BROKER,
    "group.id": GROUP,
    "auto.offset.reset": "latest",
    "fetch.min.bytes": 524288,   # 512 KB
    "fetch.wait.max.ms": 100,
    "enable.auto.commit": True,
}

consumer = Consumer(conf)
consumer.subscribe([
    "chat-room-1",
    "chat-room-2",
    "chat-room-3",
    "chat-room-4",
])

print("📥 High-throughput consumer started")

count = 0
start = time.time()

try:
    while True:
        msg = consumer.poll(0.01)
        if msg is None:
            continue
        if msg.error():
            continue

        count += 1

        if count % 5000 == 0:
            elapsed = time.time() - start
            print(f"Consumed {count} msgs | Rate: {int(count/elapsed)} msg/sec")

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
