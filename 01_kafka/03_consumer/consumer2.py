from confluent_kafka import Consumer,TopicPartition
import os,json
import dotenv

def main(topic,partition,start,end):
    tp = TopicPartition(topic = topic, partition = partition, offset = start)
    c.assign([tp])
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        value = msg.value().decode('utf-8')
        data = json.loads(value)
        print(data)
        if msg.offset() >= end:
            break

if __name__ == "__main__":
    dotenv.load_dotenv()
    topic_name = os.environ["TOPIC_NAME"]
    c = Consumer({
    'bootstrap.servers': os.environ["KAFKA_SERVER"],
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest' # earliest
    })
    main(topic=topic_name,partition=0,start=10,end=15)
