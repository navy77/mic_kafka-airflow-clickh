from confluent_kafka import Consumer
import dotenv, os

def main():
    metadata = c.list_topics(timeout=2.0)
    print("========== Cluster Metadata ==========")
    print(f"Cluster ID: {metadata.cluster_id}")
    print(f"Controller ID: {metadata.controller_id}")
    for broker_id, broker in metadata.brokers.items():
        print(f"BrokerID: {broker_id}, Host: {broker.host}, Port: {broker.port}")

    print("========== Topics Metadata ==========")
    for topic_name, topic in metadata.topics.items():
        print(f"Topic: {topic_name}")

    print("========== Partition Metadata ==========")
    for topic_name, topic in metadata.topics.items():
        print(f"Topic: {topic_name}")
        for partition in topic.partitions.values():
            print(f"  Partition {partition.id}: Leader={partition.leader}, Replicas={partition.replicas}")

if __name__ == "__main__":
    dotenv.load_dotenv()

    c = Consumer({
        'bootstrap.servers': os.environ["KAFKA_SERVER"],
        'group.id': 'mygroup',
        'auto.offset.reset': 'latest'})
    main()
