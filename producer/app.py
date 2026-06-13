import json
import random
import time

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

servers = [
    "server-01",
    "server-02",
    "server-03",
    "server-04",
    "server-05"
]

while True:

    log = {
        "server_id": random.choice(servers),
        "cpu": random.randint(20, 100),
        "memory": random.randint(20, 100),
        "disk": random.randint(20, 100),
        "response_time": random.randint(50, 3000),
        "error_count": random.randint(0, 20)
    }

    producer.send("system-logs", log)

    print(log)

    time.sleep(1)
