import json
import time
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

def main():

    with open("data/tweets.json") as file:
        data = json.load(file)

    producer = KafkaProducer(value_serializer= lambda v: json.dumps(v).encode("utf-8"))
    try:
        for tweet in data:
            producer.send("twitter", key=b'covid', value=tweet)
            time.sleep(1)
    except KafkaTimeoutError:
        logging.error("Kafka Time Out")
    finally:
        producer.close()



if __name__ == '__main__':
    main()