from confluent_kafka import Producer
import time,os,json
import dotenv

def main():
    for i in range(10000):
        message={'this is message # :':i}
        serialized_message = json.dumps(message).encode('utf-8')
        p.produce(topic = topic_name,value = serialized_message)
        print(f'message sent:{message}')
        time.sleep(1)
    p.flush()

if __name__ == "__main__":
    dotenv.load_dotenv()
    p = Producer({'bootstrap.servers': os.environ["KAFKA_SERVER"]})
    topic_name = os.environ["TOPIC_NAME"]
    while True:
        main()