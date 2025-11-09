import paho.mqtt.client as mqtt
import os
import dotenv
import json
from confluent_kafka import Producer
from queue import Queue
from threading import Thread
import logging
import sys
import time
from datetime import datetime
import pytz

class MqttToKafka:
    def __init__(self):
        dotenv.load_dotenv()

        self.mqtt_broker = os.environ["MQTT_BROKER"]
        self.mqtt_port = int(os.environ["MQTT_PORT"])
        self.sub_topics = os.environ["MQTT_SUB_TOPIC"].split(",")
        self.client_id = "mqtt_subscriber"
        self.client = mqtt.Client(self.client_id)

        self.partition_list = [os.environ["PARTITION_0"],os.environ["PARTITION_1"],os.environ["PARTITION_2"],os.environ["PARTITION_3"],os.environ["PARTITION_4"],
                               os.environ["PARTITION_5"],os.environ["PARTITION_6"],os.environ["PARTITION_7"],os.environ["PARTITION_8"],os.environ["PARTITION_9"]]
        
        self.client.max_inflight_messages_set(10000) 
        self.client.max_queued_messages_set(500000)

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.enable_logger()
        self.client.on_log = self.on_log
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)
        
        self.kafka_server = os.environ["KAFKA_SERVER"]
        self.producer = Producer({
            'bootstrap.servers': self.kafka_server,
            'batch.size': 200000,
            'linger.ms': 5,
            'queue.buffering.max.messages':1000000,
            'queue.buffering.max.kbytes': 2097152,
            'debug': 'broker,topic,msg'
        },logger = logging.getLogger())

        self.queue = Queue(maxsize = 500000)

        Thread(target=self.kafka_producer, daemon=True).start()
  
        logging.basicConfig(
            filename='log/mqtt_to_kafka.log',
            filemode="a",
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            force=True
        )

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker")
            logging.info("Connected to MQTT Broker")
            for topic in self.sub_topics:
                client.subscribe(topic.strip(), qos=0)
        else:
            print(f"Connection failed with code {rc}")
            logging.error(f"Connection failed with code {rc}")
            
    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("Disconnected from MQTT Broker")
            logging.error("Disconnected from MQTT broker")

    def on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            data = msg.payload
            self.queue.put_nowait((data,topic))

        except Exception as e:
            logging.error(f"Error on_message: {e}")

    def kafka_producer(self):
        while True:
            data, topic = self.queue.get()
            try:
                msg_topic_type_sp = topic.split("/")
                if len(msg_topic_type_sp) == 4:
                    msg_topic_type = msg_topic_type_sp[0] + msg_topic_type_sp[1] + msg_topic_type_sp[2]
                    k_topic = f'k_{msg_topic_type}'
                    # decode payload
                    try:
                        k_msg_payload = json.loads(data.decode())
                    except Exception as e:
                        logging.error(f"Cannot decode payload from topic: {topic} - Error: {e}")
                        continue
                    
                    current_time = datetime.now(pytz.timezone("Asia/Bangkok")).strftime("%Y-%m-%d %H:%M:%S")
                    k_msg = {"topic": str(topic),"ts": current_time, **k_msg_payload}
                    k_process = msg_topic_type_sp[2]
                    message = json.dumps(k_msg).encode('utf-8')

                    if k_process in self.partition_list:
                        k_partition = self.partition_list.index(k_process)
                        # produce message
                        self.producer.produce(topic=k_topic, key=k_topic, value=message, partition=k_partition)
                        self.producer.poll(0)
                    else:
                        logging.error(f"Partition not found for process: {k_process}")
                        continue
                else:
                    logging.error(f"Invalid topic format: {topic}")

            except Exception as e:
                print(f"Error Kafka producer: {e}")
                logging.error(f"Error Kafka producer: {e}")

            finally:
                self.queue.task_done()

    def on_log(self,client, userdata, level, buf):
        if level == mqtt.MQTT_LOG_DEBUG:
            logging.debug(f"MQTT Log debug: {buf}")
        elif level == mqtt.MQTT_LOG_WARNING:
            logging.warning(f"MQTT Log warning: {buf}")
        elif level == mqtt.MQTT_LOG_ERR:
            logging.error(f"MQTT Log error: {buf}")

    def run(self):
        try:
            self.client.connect(self.mqtt_broker, self.mqtt_port, 60)
            self.client.loop_start()
            while True:
                time.sleep(1)
        except Exception as e:
            print(f"Connect to broker failed: {e}")
            logging.error(f"Connect to broker failed: {e}")
            sys.exit(1)

if __name__ == "__main__":
    subscriber = MqttToKafka()
    subscriber.run()