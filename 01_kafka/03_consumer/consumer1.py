from confluent_kafka import Consumer
import os
import dotenv

def main():
    c.subscribe([topic_name])
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print('Received message: {}'.format(msg.value().decode('utf-8')))

if __name__ == "__main__":
    dotenv.load_dotenv()
    c = Consumer({
    'bootstrap.servers': os.environ["KAFKA_SERVER"],
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest' # earliest
    })
    topic_name = os.environ["TOPIC_NAME"]
    main()