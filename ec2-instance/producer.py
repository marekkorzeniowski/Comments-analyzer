#!/usr/bin/env python3

import time
from kafka import KafkaProducer

TOPIC = "comment-analyzer"
ROW_KEY = "Comment"
KAFKA_SERVER = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

with open("comments_sample.xml") as xml:
    print("File reading...")
    for line in xml:
        if line.startswith("  <row"):
            byteMessage = bytes(line.strip(), 'utf-8')
            key=bytes(ROW_KEY, 'utf-8')
            producer.send(TOPIC, key=key, value=byteMessage)

    time.sleep(5)

print("Done!")






