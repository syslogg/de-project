# THIS IS SIMULATION OF APACHE PRODUCER LOG
import time
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

def main():

    with open("data/apache.log") as file:
        data = file.read().splitlines()

    producer = KafkaProducer(value_serializer= lambda v: v.encode("utf-8"))
    try:
        for line_log in data:
            producer.send("apache_logs", key=b'apache', value=line_log)
            time.sleep(5)
    except KafkaTimeoutError:
        logging.error("Kafka Time Out")
    finally:
        producer.close()



if __name__ == '__main__':
    main()