import pickle
from kafka import KafkaConsumer

consumer = KafkaConsumer("python_test1", bootstrap_servers=["localhost:9092"], auto_offset_reset="latest")

for msg in consumer:
    key = pickle.loads(msg.key)
    value = pickle.loads(msg.value)
    offset = msg.offset
    partition = msg.partition
    topic = msg.topic
    if key.lower() == "end":
        break
    print(f"{topic}: partition:{partition}-offset:{offset} value:{value} key: {key}")

consumer.close()
