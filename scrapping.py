import pandas as pd
import json
import os
from selenium import webdriver
from typing import Set, List, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

class Scrapper:
    def __init__(self, driver, cache_file: Optional[str] = None):
        self.driver = driver
        self.cache_file = cache_file

    def get_info(self, scrapped_addresses: Set[str]) -> dict:
        if (self.cache_file is not None):
            try:
                with open(self.cache_file, 'r') as f:
                    scrapped_info = json.load(f)
                return scrapped_info
            except FileNotFoundError:
                print(f"Cache file {self.cache_file} not found. Proceeding without cache.") 

        scrapped_info = {
            hex_address: {} for hex_address in scrapped_addresses
        }
        for hex_address in scrapped_addresses:
            # Open URL
            self.driver.get(f'https://debank.com/profile/{hex_address}/')
            try:
                # Wait for the avatar image to load
                avatar_element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'img[class*="db-user-avatar"]'))
                )
                # Extract the image URL
                image_url = avatar_element.get_attribute("src")
                print("Extracted image URL:", image_url)
                scrapped_info[hex_address]['image_url'] = image_url
            except Exception as e:
                print(f"An error occurred: {e}")
            
            try:
                # Wait for the element to load
                element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "db-user-tag-content"))
                )
                # Extract and print the text
                print("Extracted text:", element.text)
                scrapped_info[hex_address]['tag'] = element.text
            except Exception as e:
                print(f"An error occurred: {e}")

            try:
                # Wait for the username element to load (handle dynamic class names)
                username_element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div[class*="HeaderInfo_uid"]'))
                )
                # Extract the username text
                username = username_element.text
                print("Extracted username:", username)
                scrapped_info[hex_address]['username'] = username
            except Exception as e:
                print(f"An error occurred while extracting username for {hex_address}: {e}")

        return scrapped_info

def parse_scrapped_info(file_path: str) -> List[dict]:
    """
    Функция парсит scrapped_info.json и преобразует данные в новую схему protocols.

    :param file_path: Путь к JSON-файлу
    :return: Список протоколов в новой схеме
    """
    with open(file_path, 'r') as f:
        scrapped_info = json.load(f)
    
    protocols = []
    for hex_address, info in scrapped_info.items():
        # Извлечение данных
        tag = info.get('tag', info.get('username', 'Unknown'))
        image_url = info.get('image_url', 'https://cryptologos.cc/logos/ethereum-eth-logo.png')
        name = tag.split(':')[1] if ':' in tag else info.get('username', 'Unknown')
        
        # Формирование объекта протокола
        protocol = {
            'name': name,
            'hex_address': hex_address,
            'description': f'{tag} - No additional description available.',
            'url': f'https://debank.com/profile/{hex_address}',
            'image_url': image_url
        }
        protocols.append(protocol)
    
    return protocols

def read_addresses_from_csv(file_path: str, threshold_operations = 10) -> List[str]:
    """
    Функция читает CSV-файл с помощью pandas и извлекает уникальные адреса из колонки '.To'.

    :param file_path: Путь к CSV-файлу
    :return: Список уникальных адресов из колонки '.To'
    """
    try:
        # Чтение CSV-файла
        data = pd.read_csv(file_path)
        
        if 'To' not in data.columns or 'Method' not in data.columns:
            raise ValueError("Не найдены необходимые колонки в файле.")
        filtered_data = data[~data['Method'].isin(['Approve', 'Execute'])]
        address_counts = filtered_data[['To', 'Method']].value_counts()

        # Фильтрация адресов по количеству операций
        filtered_addresses = address_counts[address_counts >= threshold_operations]
        
        # Преобразование в список кортежей (адрес, количество операций)
        result = list(filtered_addresses.items())

        return result
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        return []

if __name__ == "__main__":
    file_path = "dataset/etherium/full_data.csv"
    cache_file = "scrapped_info.json"
    topInteractions = read_addresses_from_csv(file_path)
    scrapped_addresses = set()
    for (address, _type), count in topInteractions:
        print(f"Address: {address}, Count: {count}, Type: {_type}")
        print("-" * 40)
        scrapped_addresses.add(address)
    driver = webdriver.Chrome()
    scrapper = Scrapper(driver, cache_file=cache_file)
    scrapped_info = scrapper.get_info(scrapped_addresses)
    if (not os.path.exists(cache_file)):
        with open(cache_file, 'w') as f:
            json.dump(scrapped_info, f)
    for (address, _type), count in topInteractions:
        print(f"Address: {address}, Count: {count}, Type: {_type}")
        print(f"Info: {scrapped_info.get(address, {}).get('tag', 'No info found')}")
        print("-" * 40)
    driver.close()
