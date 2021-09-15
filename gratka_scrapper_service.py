import requests
import time
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import json

GRATKA_URL = "https://gratka.pl/nieruchomosci/mieszkania"


class ScrapperService:
    def __init__(self, pause_time, kafka_addr):
        self.pause_time = pause_time
        self.producer = KafkaProducer(bootstrap_servers=kafka_addr)

    def get_soup_from_url(self, url):
        time.sleep(self.pause_time)
        page = requests.get(url)
        return BeautifulSoup(page.content, "html.parser")

    def get_elements_main_page(self):
        soup = self.get_soup_from_url(GRATKA_URL)
        return soup.find_all("div", class_="teaserUnified__mainInfo")

    def get_offer_url(self, main_info_element):
        link_info_element = main_info_element.find("a", class_="teaserUnified__anchor")
        return link_info_element['href']

    def get_price(self, params, offer_soup):
        price = offer_soup.find("p", class_="creditCalc__price")
        if price:
            params['price'] = price.get_text()

    def get_details(self, params, offer_soup):
        raw_parameters = offer_soup.find("ul", class_="parameters__singleParameters").find_all("li")
        for raw_parameter in raw_parameters:
            try:
                type = raw_parameter.find("span").get_text()
                value = raw_parameter.find("b").get_text()
                params[type] = value
            except:
                pass


    def send_data(self, data):
        future = self.producer.send('raw_data', json.dumps(data).encode('utf-8'))
        result = future.get(timeout=60)
        print(result)

    def start(self):
        main_info_elements = self.get_elements_main_page()
        for main_info_element in main_info_elements:
            offer_url = self.get_offer_url(main_info_element)
            offer_soup = self.get_soup_from_url(offer_url)
            params = {}

            self.get_price(params, offer_soup)
            self.get_details(params, offer_soup)

            self.send_data(params)


