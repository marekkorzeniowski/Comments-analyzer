
import time
import _thread

from kafka import KafkaProducer



def kafka_producer(topic, key_row, path):
    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    with open(path) as xml:
        print(f"File reading into {topic}...")
        for line in xml:
            if line.startswith("  <row"):
                byteMessage = bytes(line.strip(), 'utf-8')
                key = bytes(key_row, 'utf-8')
                producer.send(topic, key=key, value=byteMessage)
                # time.sleep(1)

        time.sleep(10)

    print("Done!")


if __name__ == '__main__':

    _thread.start_new_thread(kafka_producer, ("post-analyzer", "Post", "Posts.xml", ))
    kafka_producer("comment-analyzer", "Comment", "Comments.xml")









