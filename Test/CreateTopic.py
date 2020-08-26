from kafka.admin import KafkaAdminClient, NewTopic

kafka_admin_client = KafkaAdminClient(
    bootstrap_servers=["localhost:9092"])

try:
    topics = [NewTopic(name="MCC_FILE_DATA", num_partitions=1, replication_factor=1)]

    kafka_admin_client.create_topics(new_topics=topics, validate_only=False)
    # kafka_admin_client.delete_topics(["MCC_FILE_DATA"])

    print(kafka_admin_client.list_topics())

except Exception as e:
    raise e
finally:
    kafka_admin_client.close()

