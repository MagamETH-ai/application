import pandas as pd
import yadisk
import json
import time
import glob
import os
from selenium import webdriver
from dotenv import load_dotenv
from typing import Set, List, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

class EtherscanScrapper:
    def __init__(self, driver, download_dir: str = "."):
        self.driver = driver
        print(f"Token: {os.getenv('YADISK_TOKEN')}")
        self.yadisk = yadisk.Client(token=os.getenv('YADISK_TOKEN'))
        print(f"Yadisk client initialized. Instance: {self.yadisk}") 
        self.download_dir = download_dir

    def get_info(self, duna_addresses: Set[str]) -> dict:
        """
        Скачивает CSV-файлы транзакций для каждого адреса с сайта Etherscan.

        :param duna_addresses: Набор адресов для парсинга
        :return: Словарь с информацией о статусе скачивания для каждого адреса
        """
        scrapped_info = {
            hex_address: {"status": "pending"} for hex_address in duna_addresses
        }

        for hex_address in duna_addresses:
            try:
                # Проверяем, существует ли файл на Яндекс.Диске
                yadisk_path = f"/exports/{hex_address}_transactions.csv"
                if self.yadisk.exists(yadisk_path):
                    print(f"File for user {hex_address} already exists on Yandex.Disk. Skipping...")
                    scrapped_info[hex_address]["status"] = "already_exists"
                    continue

                # Открываем URL для адреса
                self.driver.get(f'https://etherscan.io/txs?a={hex_address}')

                # Ждём появления информации о страницах
                total_pages_element = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.pagination > li:last-child > a'))
                )

                # Извлекаем общее количество страниц
                total_pages = int(total_pages_element.get_attribute("href").split("p=")[-1])
                print(f"Total pages for {hex_address}: {total_pages}")

                # Счётчик ошибок для текущего адреса
                error_count = 0
                threshold = total_pages // 3
                
                # Проходим по всем страницам
                for page in range(1, total_pages + 1):
                    try:
                        # Открываем текущую страницу
                        self.driver.get(f'https://etherscan.io/txs?a={hex_address}&p={page}')
                        # Ждём появления кнопки экспорта
                        export_button = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.ID, "btnExportQuickTransactionListCSV"))
                        )

                        # Нажимаем на кнопку экспорта
                        export_button.click()
                        print(f"Clicked export button for address: {hex_address}")
                        
                        # Ожидание завершения скачивания файла
                        self._wait_for_download(hex_address, page=page)
                    except Exception as e:
                        error_count += 1
                        print(f"An error occurred on page {page} for address {hex_address}: {e}")

                        # Если количество ошибок превышает порог, прерываем обработку
                        if error_count > threshold:
                            print(f"Too many errors for address {hex_address}. Marking as failed.")
                            scrapped_info[hex_address]["status"] = "failed"
                            scrapped_info[hex_address]["errors"] = error_count
                            break

                # Если ошибок меньше порога, считаем обработку успешной
                if error_count <= threshold:
                    self.merge_csv_by_user(hex_address)
                    scrapped_info[hex_address]["status"] = "success"
                    scrapped_info[hex_address]["errors"] = error_count
            except Exception as e:
                print(f"An error occurred for address {hex_address}: {e}")
                scrapped_info[hex_address]["status"] = "failed"
            
        return scrapped_info

    def merge_csv_by_user(self, hex_address: str):
        """
        Объединяет все CSV-файлы для конкретного пользователя в один CSV-файл и удаляет исходные файлы.

        :param hex_address: HEX-адрес пользователя
        """
        # Ищем все файлы, относящиеся к данному пользователю
        user_files = glob.glob(os.path.join(self.download_dir, f"{hex_address}_transactions_*.csv"))
        
        if not user_files:
            print(f"No CSV files found for user {hex_address}.")
            return

        print(f"Found {len(user_files)} CSV files for user {hex_address}. Merging...")
        
        # Читаем и объединяем все файлы
        combined_df = pd.concat([pd.read_csv(file) for file in user_files], ignore_index=True)
        
        # Сохраняем объединённый файл
        output_file = os.path.join(self.download_dir, f"{hex_address}_transactions.csv")
        combined_df.to_csv(output_file, index=False)
        print(f"CSV files for user {hex_address} have been merged into {output_file}.")
        # Load to yadisk

        # Загружаем файл на Яндекс.Диск
        yadisk_path = f"/exports/{hex_address}_transactions.csv"
        try:
            self.yadisk.upload(output_file, yadisk_path, overwrite=True)
            os.remove(output_file)  # Удаляем локальный файл после загрузки
            print(f"File {output_file} has been uploaded to Yandex.Disk at {yadisk_path}.")
        except Exception as e:
            print(f"Failed to upload file {output_file} to Yandex.Disk: {e}")

        # Удаляем исходные файлы
        for file in user_files:
            os.remove(file)
            print(f"Deleted file: {file}")

    def _wait_for_download(self, hex_address: str, page = 1, timeout: int = 30):
        """
        Ожидает завершения скачивания файла в указанной директории.

        :param hex_address: HEX-адрес для проверки имени файла
        :param timeout: Максимальное время ожидания в секундах
        """
        start_time = time.time()
        downloaded_file = None

        time.sleep(0.5)
        while True:
            # Ищем последний скачанный файл в папке загрузок
            list_of_files = glob.glob(os.path.join(self.download_dir, '*'))
            if list_of_files:
                latest_file = max(list_of_files, key=os.path.getctime)
                if latest_file.endswith(".csv") and "crdownload" not in latest_file:
                    downloaded_file = latest_file
                    break

            # Проверяем таймаут
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Download timed out for address: {hex_address}")

            time.sleep(1e-2)
        time.sleep(0.5)
        # Переименование файла
        if downloaded_file:
            new_file_name = f"{hex_address}_transactions_{page}.csv"
            new_path = os.path.join(self.download_dir, new_file_name)
            os.rename(downloaded_file, new_path)
            print(f"File renamed to: {new_file_name}")

def setup_chrome_driver(download_dir: str) -> webdriver.Chrome:
    chrome_options = Options()
    prefs = {
        "download.default_directory": os.path.abspath(download_dir),  # Указываем абсолютный путь
        "download.prompt_for_download": False,  # Отключаем запрос на подтверждение загрузки
        "directory_upgrade": True,  # Разрешаем обновление директории
        "safebrowsing.enabled": True  # Включаем безопасное скачивание
    }
    chrome_options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome(options=chrome_options)

def read_addresses_from_csv(file_path: str) -> List[str]:
    try:
        data = pd.read_csv(file_path)
        result = data['account'].unique().tolist()
        return result
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        return []

if __name__ == "__main__":
    load_dotenv(".env")
    addresses = read_addresses_from_csv('airdrop_wallets.csv')
    print(*addresses, sep='\n')
    download_dir = os.path.join(os.getcwd(), "exports")
    driver = setup_chrome_driver(download_dir)
    scrapper = EtherscanScrapper(driver, download_dir="exports")
    scrapped_info = scrapper.get_info(addresses)
    print(scrapped_info)
    driver.close()
