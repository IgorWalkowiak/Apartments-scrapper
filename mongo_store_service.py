from kafka import KafkaConsumer
import json

def get_database():
    import pymongo
    import credentials
    CONNECTION_STRING = "mongodb+srv://"+credentials.username+":"+credentials.password+"@cluster0.v0bqn.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
    client = pymongo.MongoClient(CONNECTION_STRING)
    return client['scrapper']

class StoreService:
    def __init__(self, kafka_addr):
        self.kafka_consumer = KafkaConsumer('parsed_data', bootstrap_servers=kafka_addr,
                                    value_deserializer=lambda m: json.loads(m.decode('ascii')))
        db = get_database()
        self.collection = db['apartments']

    def start(self):
        for message in self.kafka_consumer:
            print("received message!")
            self.collection.insert_one(message.value)

service = StoreService('localhost:9092')
service.start()