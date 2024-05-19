from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.errors import UnknownTopicOrPartitionError
import json

def create_topic(topic, bootstrap_servers='localhost:9092'):
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
    try:
        topic_list = [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{topic}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic}' already exists, skipping creation.")

def delete_topics(topic_names):
    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')
    try:
        admin_client.delete_topics(topics=topic_names)
        print("Topic Deleted Successfully")
    except UnknownTopicOrPartitionError as e:
        print("Topic Doesn't Exist")
    except  Exception as e:
        print(e)

def produce_to_topic(data, topic, bootstrap_servers='localhost:9092'):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send(topic, value=data)
    producer.flush()