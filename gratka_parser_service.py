from kafka import KafkaConsumer, KafkaProducer
import json

class ParserService:
    def __init__(self, kafka_addr):
        self.kafka_consumer = KafkaConsumer('raw_data', bootstrap_servers=kafka_addr, value_deserializer=lambda m: json.loads(m.decode('ascii')))
        self.producer = KafkaProducer(bootstrap_servers=kafka_addr)

    def start(self):
        for message in self.kafka_consumer:
            self.handle_message(message)

    @staticmethod
    def remove_characters(string_to_modifie, disallowed_characters):
        for character in disallowed_characters:
            string_to_modifie = string_to_modifie.replace(character, "")
        return string_to_modifie

    def parse_price(self, content):
        try:
            new_price = self.remove_characters(content['price'], ' zł')
            content['price'] = int(new_price)
        except Exception as exception:
            print("zawiodlem parse_price", exception)

    def parse_location(self, content):
        try:
            new_location = self.remove_characters(content['Lokalizacja'], ' \n')
            content['Lokalizacja'] = new_location
        except Exception as exception:
            print("zawiodlem parse_location", exception)

    def parse_area(self, content):
        try:
            new_area = self.remove_characters(content['Powierzchnia w m2'], '\n ')
            new_area = new_area[:-2] # last to characters stands for 'm2'
            new_area = new_area.translate(new_area.maketrans(",","."))
            print(new_area)
            content['Powierzchnia w m2'] = float(new_area)
        except Exception as exception:
            print("zawiodlem parse_area", exception)

    def send_data(self, data):
        future = self.producer.send('parsed_data', json.dumps(data).encode('utf-8'))
        result = future.get(timeout=60)
        print(result)

    def handle_message(self, message):
        content = message.value
        self.parse_price(content)
        self.parse_location(content)
        self.parse_area(content)
        print(content)
        self.send_data(content)

service = ParserService('localhost:9092')
service.start()
