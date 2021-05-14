from kafka import KafkaConsumer

consumer = KafkaConsumer('twitter')

for msg in consumer:
    print(msg)
