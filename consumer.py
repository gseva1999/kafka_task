from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from time import sleep
import json


topic = "word-num"
broker = "localhost:9092"
group_id = "consumer-1"


class MyKafkaConsumer:
    def __init__(self, consumer, topic):
        self.consumer = consumer
        self.topic = topic

    def poll_partition(self, partition):
        tp = TopicPartition(self.topic, partition)
        self.consumer.assign([tp])
        part_msg = self.consumer.poll(2010, max_records=1)
        return part_msg

    def position(self, partition):
        tp = TopicPartition(self.topic, partition)
        return self.consumer.position(tp)

    def commit(self):
        self.consumer.commit()

    def seek(self, partition, offset):
        tp = TopicPartition(self.topic, partition)
        self.consumer.assign([tp])
        self.consumer.seek(partition=tp, offset=offset)


def create_consumer():
    """Create Kafka consumer"""
    consumer = KafkaConsumer(bootstrap_servers=broker, auto_offset_reset='latest',
                             enable_auto_commit=False, group_id=group_id)
    return MyKafkaConsumer(consumer, topic)


my_consumer = create_consumer()


while True:
    print("Listening...")
    number = my_consumer.poll_partition(0)
    key = ""
    for tp, messages in number.items():
        for message in messages:
            key = message.value.decode('utf-8')
    my_consumer.consumer.commit()
    key_position = my_consumer.position(0)
    # my_consumer.seek(0, key_position)
    my_consumer.seek(1, key_position-1)
    word = my_consumer.poll_partition(1)
    value = ""
    for tp, messages in word.items():
        for message in messages:
            value = message.value.decode('utf-8')
    # my_consumer.seek(1, key_position)
    # my_consumer.consumer.commit()
    sample = {key: value}
    print(sample)
    with open('result2.json', 'a') as fp:
        json.dump(sample, fp)
        fp.write('\n')
    my_consumer.consumer.commit()
