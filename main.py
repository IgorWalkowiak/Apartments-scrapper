import requests
from bs4 import BeautifulSoup
import time

gratka_URL = "https://gratka.pl/nieruchomosci/mieszkania"
gratka_page = requests.get(gratka_URL)
soup = BeautifulSoup(gratka_page.content, "html.parser")
main_info_elements = soup.find_all("div", class_="teaserUnified__mainInfo")
for main_info_element in main_info_elements:
    time.sleep(1)
    link_info_element = main_info_element.find("a", class_="teaserUnified__anchor")
    offer_URL = link_info_element['href']
    offer_page = requests.get(offer_URL)
    soup = BeautifulSoup(offer_page.content, "html.parser")
    params = {}
    price = soup.find("p", class_="creditCalc__price")
    if price:
        params['Price'] = price.get_text()
    print("______________")
    raw_parameters = soup.find("ul", class_="parameters__singleParameters").find_all("li")
    for raw_parameter in raw_parameters:
        try:
            type = raw_parameter.find("span").get_text()
            value = raw_parameter.find("b").get_text()
            params[type] = value
        except:
            pass
    if params['Lokalizacja']:
        params['Lokalizacja'] = params['Lokalizacja'].replace(' ','')
        params['Lokalizacja'] = params['Lokalizacja'].replace('\n','')
    print(params)
    print(link_info_element['href'])
    print("_____")
