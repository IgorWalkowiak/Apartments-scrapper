from kafka import KafkaConsumer
import json

kafka_addr = 'localhost:9092'

kafka_consumer = KafkaConsumer('parsed_data', bootstrap_servers=kafka_addr,
                                    value_deserializer=lambda m: json.loads(m.decode('ascii')))


for message in kafka_consumer:
    print(type(message.value['Powierzchnia w m2']))