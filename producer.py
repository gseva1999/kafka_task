from kafka import KafkaConsumer, KafkaProducer
import random
import string
from time import sleep


topic = 'word-num'
bootstrap_servers = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
consumer = KafkaConsumer(
    topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')

while True:
    num = str(random.randint(1, 100))
    future = producer.send(topic, num.encode('utf-8'), partition=0)
    print(num)
    word = ''.join(random.choice(string.ascii_lowercase) for _ in range(8))
    future2 = producer.send(topic, word.encode('utf-8'), partition=1)
    print(word)
    sleep(2)
