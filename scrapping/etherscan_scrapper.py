import shutil
import requests
import pandas as pd
from typing import List
import concurrent.futures
from queue import Queue
from threading import Lock
from tqdm import tqdm
import yadisk
import atexit
import json
import time
import glob
import sys
import os
from selenium import webdriver
from dotenv import load_dotenv
from typing import List
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

from settings import logger

def setup_chrome_driver(download_dir: str, proxy: str = None) -> webdriver.Chrome:
    chrome_options = Options()
    prefs = {
        "download.default_directory": os.path.abspath(download_dir),  # Указываем абсолютный путь
        "download.prompt_for_download": False,  # Отключаем запрос на подтверждение загрузки
        "directory_upgrade": True,  # Разрешаем обновление директории
        "safebrowsing.enabled": True  # Включаем безопасное скачивание
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Если указан прокси, добавляем его в настройки
    if proxy:
        chrome_options.add_argument(f'--proxy-server={proxy}')

    return webdriver.Chrome(options=chrome_options)

def read_addresses_from_csv(file_path: str) -> List[str]:
    try:
        data = pd.read_csv(file_path)
        result = data['account'].unique().tolist()
        return result
    except FileNotFoundError:
        logger.info(f"Файл {file_path} не найден.")
    except Exception as e:
        logger.info(f"Произошла ошибка: {e}")
        return []

class EtherscanScrapperManager:
    def __init__(
            self,
            addresses: List[str],
            num_workers: int,
            download_dir: str = "exports",
            cache_file: str = "cache.json",
            proxies: List[str] = None
    ):
        self.addresses = addresses
        self.num_workers = num_workers
        self.download_dir = download_dir
        self.cache_file = cache_file
        self.cache_list: List[str] = self.get_cache_list(cache_file)
        self.queue = Queue()
        self.lock = Lock()
        self.proxies = proxies if proxies else [None] * num_workers

        # Заполняем очередь задачами
        for address in addresses:
            self.queue.put(address)
        
        # Инициализируем tqdm для отображения прогресса
        self.progress_bar = tqdm(total=len(addresses), desc="Processing addresses", unit="address")

    def get_cache_list(self, cache_file):
        """
        Загружает список кэша из файла.
        """
        if not os.path.exists(cache_file):
            return []
        with open(cache_file, "r") as f:
            stored_data = json.load(f)
        return stored_data.get("cache_list", [])
    
    def save_cache(self):
        """
        Сохраняет список кэша в файл.
        """
        with open(self.cache_file, "w") as cache_file:
            json.dump({"cache_list": self.cache_list}, cache_file)

    def worker(self, worker_id: int):
        """
        Воркер, который обрабатывает задачи из очереди.
        """
        # Назначаем прокси для текущего воркера
        proxy = self.proxies[worker_id] if worker_id < len(self.proxies) else None

        # Создаём отдельную директорию для воркера
        worker_download_dir = os.path.join(self.download_dir, f"worker_{worker_id}")
        os.makedirs(worker_download_dir, exist_ok=True)

        # Инициализируем Chrome и Scrapper
        driver = setup_chrome_driver(worker_download_dir, proxy=proxy)
        scrapper = EtherscanScrapper(driver, download_dir=worker_download_dir)

        while not self.queue.empty():
            try:
                # Берём задачу из очереди
                with self.lock:
                    if self.queue.empty():
                        break
                    hex_address = self.queue.get()

                # Проверяем, есть ли адрес в кэше
                if hex_address in self.cache_list:
                    logger.info(f"Address {hex_address} is already cached. Skipping...")
                    self.progress_bar.update(1)
                    continue

                logger.info(f"Worker {worker_id} processing address: {hex_address}")
                result = scrapper.get_info(hex_address)

                # Если успешно обработано, добавляем в кэш
                if result[hex_address]["status"] in ("success", "already_exists"):
                    with self.lock:
                        self.cache_list.append(hex_address)
                
                # Обновляем прогресс
                self.progress_bar.update(1)
            except Exception as e:
                logger.info(f"Worker {worker_id} encountered an error: {e}")
            finally:
                self.queue.task_done()

        driver.close()
        logger.info(f"Worker {worker_id} finished.")

    def run(self):
        """
        Запускает воркеров для обработки задач.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [executor.submit(self.worker, worker_id) for worker_id in range(self.num_workers)]
            concurrent.futures.wait(futures)
        self.progress_bar.close()
        self.save_cache()

class EtherscanScrapper:
    def __init__(self, driver, download_dir: str):
        self.driver = driver
        logger.info(f"Token: {os.getenv('YADISK_TOKEN')}")
        self.yadisk = yadisk.Client(token=os.getenv('YADISK_TOKEN'))
        logger.info(f"Yadisk client initialized. Instance: {self.yadisk}")
        self.download_dir = download_dir
        logger.info(f"Initialized EtherscanScrapper with download directory: {self.download_dir}")

    def get_info(self, hex_address: str) -> dict:
        """
        Скачивает CSV-файлы транзакций для каждого адреса с сайта Etherscan.

        :param duna_addresses: Набор адресов для парсинга
        :return: Словарь с информацией о статусе скачивания для каждого адреса
        """
        scrapped_info = {hex_address: {"status": "pending"}}
        try:
            # Проверяем, существует ли файл на Яндекс.Диске
            yadisk_path = f"/exports/{hex_address}_transactions.csv"
            if self.yadisk.exists(yadisk_path):
                logger.info(f"File for user {hex_address} already exists on Yandex.Disk. Skipping...")
                scrapped_info[hex_address]["status"] = "already_exists"
                return scrapped_info

            # Открываем URL для адреса
            self.driver.get(f'https://etherscan.io/txs?a={hex_address}')

            # Ждём появления информации о страницах
            total_pages_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.pagination > li:last-child > a'))
            )

            # Извлекаем общее количество страниц
            total_pages = int(total_pages_element.get_attribute("href").split("p=")[-1])
            logger.info(f"Total pages for {hex_address}: {total_pages}")

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
                    logger.info(f"Clicked export button for address: {hex_address}")
                    
                    # Ожидание завершения скачивания файла
                    self._wait_for_download(hex_address, page=page)
                except Exception as e:
                    error_count += 1
                    logger.info(f"An error occurred on page {page} for address {hex_address}: {e}")

                    # Если количество ошибок превышает порог, прерываем обработку
                    if error_count > threshold:
                        logger.info(f"Too many errors for address {hex_address}. Marking as failed.")
                        scrapped_info[hex_address]["status"] = "failed"
                        scrapped_info[hex_address]["errors"] = error_count
                        break

            # Если ошибок меньше порога, считаем обработку успешной
            if error_count <= threshold:
                result = self.merge_csv_by_user(hex_address)
                if (result):
                    scrapped_info[hex_address]["status"] = "success"
                    scrapped_info[hex_address]["errors"] = error_count
                else:
                    scrapped_info[hex_address]["status"] = "failed"
                    scrapped_info[hex_address]["errors"] = error_count
        except Exception as e:
            logger.info(f"An error occurred for address {hex_address}: {e}")
            scrapped_info[hex_address]["status"] = "failed"

        return scrapped_info

    def merge_csv_by_user(self, hex_address: str) -> bool:
        """
        Объединяет все CSV-файлы для конкретного пользователя в один CSV-файл и удаляет исходные файлы.

        :param hex_address: HEX-адрес пользователя
        """
        # Ищем все файлы, относящиеся к данному пользователю
        user_files = glob.glob(os.path.join(self.download_dir, f"{hex_address}_transactions_*.csv"))
        
        if not user_files:
            logger.info(f"No CSV files found for user {hex_address}.")
            return False

        logger.info(f"Found {len(user_files)} CSV files for user {hex_address}. Merging...")
        
        # Читаем и объединяем все файлы
        combined_df = pd.concat([pd.read_csv(file) for file in user_files], ignore_index=True)
        
        # Сохраняем объединённый файл
        output_file = os.path.join(self.download_dir, f"{hex_address}_transactions.csv")
        combined_df.to_csv(output_file, index=False)
        logger.info(f"CSV files for user {hex_address} have been merged into {output_file}.")
        # Load to yadisk

        # Загружаем файл на Яндекс.Диск
        yadisk_path = f"/exports/{hex_address}_transactions.csv"
        try:
            self.yadisk.upload(output_file, yadisk_path, overwrite=True)
            os.remove(output_file)  # Удаляем локальный файл после загрузки
            logger.info(f"File {output_file} has been uploaded to Yandex.Disk at {yadisk_path}.")
        except Exception as e:
            logger.info(f"Failed to upload file {output_file} to Yandex.Disk: {e}")
            return False

        # Удаляем исходные файлы
        for file_path in user_files:
            try:
                os.remove(file_path)
            except OSError as e:
                logger.info(f"Error deleting file {file_path}: {e}")
        return True

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
            logger.info(f"File renamed to: {new_file_name}")

def fetch_proxies(proxy_file: str = "https.txt", num_proxies: int = 8, test_url: str = "https://etherscan.io/") -> List[str]:
    """
    Загружает список актуальных HTTP-прокси, проверяет их доступность и возвращает N самых быстрых.

    :param proxy_file: Имя файла для сохранения списка прокси.
    :param num_proxies: Количество прокси, которые нужно вернуть.
    :param test_url: URL для проверки доступности прокси.
    :return: Список рабочих и быстрых прокси-серверов.
    """
    try:
        # Загружаем список прокси
        # os.system(f"curl -sL https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt -o {proxy_file}")
        # logger.info(f"Proxies downloaded to {proxy_file}.")
        
        # Читаем прокси из файла
        with open(proxy_file, "r") as f:
            proxies = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(proxies)} proxies.")
        logger.info(proxies)

        # Проверяем доступность прокси
        working_proxies = []
        for proxy in tqdm(proxies, desc="Checking proxies from https proxy list"):
            try:
                headers = {
                    "User-Agent": "curl/8.7.1"
                }
                proxies_dict = {"http": proxy, "https": proxy}
                start_time = time.time()
                response = requests.get(test_url, proxies=proxies_dict, headers=headers, timeout=3)
                latency = time.time() - start_time
                if response.status_code == 200:
                    working_proxies.append((proxy, latency))
                    logger.info(f"Proxy {proxy} is working. Latency: {latency:.2f}s")
                else:
                    logger.info(f"Proxy {proxy} failed. Response Status Code: {response.status_code}")
            except Exception as e:
                logger.info(f"Proxy {proxy} failed. Exception: {e}")
                continue

        # Если нет рабочих прокси
        if not working_proxies:
            logger.info("No working proxies found.")
            return []

        # Сортируем прокси по времени отклика и выбираем N самых быстрых
        working_proxies.sort(key=lambda x: x[1])
        fastest_proxies = [proxy for proxy, _ in working_proxies[:num_proxies]]
        logger.info(f"Selected {len(fastest_proxies)} fastest proxies.")
        return fastest_proxies

    except Exception as e:
        logger.info(f"Failed to fetch proxies: {e}")
        return []

def onExit(manager: EtherscanScrapperManager):
    """
    Выполняется при выходе из программы. Сохраняет кэш и удаляет все файлы и папки внутри exports.
    """
    manager.save_cache()

    # Удаляем все файлы и папки внутри exports
    try:
        shutil.rmtree(manager.download_dir)
        logger.info(f"All files and folders inside {manager.download_dir} have been deleted.")
    except Exception as e:
        logger.info(f"Error deleting directory {manager.download_dir}: {e}")

def main():
    logger.info(f'GIL disabled: {not sys._is_gil_enabled()}')

    load_dotenv(".env")
    addresses = read_addresses_from_csv('airdrop_wallets.csv')
    logger.info("\n".join(addresses))

    download_dir = os.path.join(os.getcwd(), "exports")
    os.makedirs(download_dir, exist_ok=True)

    # Получаем список прокси
    proxies = fetch_proxies()
    assert len(proxies) >= 8
    # Указываем количество воркеров равное кол-ву рабочих прокси + 1
    num_workers = min(len(proxies), 16)

    manager = EtherscanScrapperManager(
        addresses = addresses,
        num_workers = num_workers,
        download_dir = download_dir,
        proxies=proxies
    )
    atexit.register(onExit, manager)
    manager.run()

if __name__ == "__main__":
    main()
