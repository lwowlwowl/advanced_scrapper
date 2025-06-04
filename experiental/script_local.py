# main_scraper.py

import csv
import threading
import queue
import os
import sys
import pandas as pd
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from importlib import import_module

def prGreen(skk): print("\033[92m {}\033[00m" .format(skk))
def prRed(skk): print("\033[91m {}\033[00m" .format(skk))


# Number of worker threads
num_threads = 20

# Selenium WebDriver options
def get_selenium_options():
    options = Options()
    options.set_preference("permissions.default.image", 2)
    options.set_preference("javascript.enabled", False)
    options.set_preference("dom.ipc.plugins.enabled.libflashplayer.so", False)
    options.add_argument("-headless")  # Run in headless mode
    return options

# Function to scrape article content
def scrape_article_content(url_queue, result_queue, extractor_module):
    options = get_selenium_options()
    service = Service(executable_path='geckodriver')
    driver = webdriver.Firefox(service=service, options=options)
    driver.set_page_load_timeout(30)
    while True:
        try:
            url = url_queue.get_nowait()
        except queue.Empty:
            break
        filename = url.split('/')[-1].split('.html')[0]
        try:
            driver.get(url)
            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            # Extract data using the extractor module
            data = extractor_module.extract_article_data(soup)

            # Check if the title is empty
            title = data.get('title', '')
            if not title:
                error_message = "Title is empty"
                data = {
                    'url': url,
                    'error': error_message
                }
                # Put the error into the result queue
                result_queue.put(('failed', data))
                prRed(f"FAIL {url} : {error_message}")
                continue  # Skip to the next URL

            data['url'] = url
            # Put the result into the result queue
            result_queue.put(('success', data))
            prGreen(f"success: {url}")
        except Exception as e:
            error_message = str(e)
            data = {
                'url': url,
                'error': error_message
            }
            # Put the error into the result queue
            result_queue.put(('failed', data))
            print(f"Failed to scrape {filename}: {error_message}")
        finally:
            url_queue.task_done()
    driver.quit()

def main():
    # Parse command-line arguments
    # parser = argparse.ArgumentParser(description='Web Scraper')
    # parser.add_argument('--website', type=str, required=True, help='Specify the website to scrape (e.g., yahoo, site2)')
    # args = parser.parse_args()

    website = "yfin"

    # Dynamically import the extractor module
    try:
        extractor_module = import_module(f'extractors.{website}')
    except ImportError:
        print(f"Extractor module for website '{website}' not found.")
        sys.exit(1)

    # Read the CSV file to get the URLs
    input_csv_file = f'yahoo_links_new.csv'
    if not os.path.exists(input_csv_file):
        print(f"Input CSV file '{input_csv_file}' not found.")
        sys.exit(1)

    df_links = pd.read_csv(input_csv_file)
    # Read already scraped URLs from success and failed CSV files
    scraped_urls = set()
    success_csv_file = f'success_articles_{website}.csv'
    failed_csv_file = f'failed_articles_{website}.csv'
    success_csv_fields = ['url', 'datetime', 'ticker_symbols', 'author', 'source', 'source_url', 'title', 'article']
    failed_csv_fields = ['url', 'error']
    if os.path.exists(success_csv_file):
        df_success_existing = pd.read_csv(success_csv_file)
        scraped_urls.update(df_success_existing['url'].astype(str).tolist())
    else:
        df_success_existing = pd.DataFrame(columns=success_csv_fields)
    if os.path.exists(failed_csv_file):
        df_failed_existing = pd.read_csv(failed_csv_file)
        scraped_urls.update(df_failed_existing['url'].astype(str).tolist())
    else:
        df_failed_existing = pd.DataFrame(columns=failed_csv_fields)
    # Filter out already scraped URLs
    df_links = df_links[~df_links['url'].astype(str).isin(scraped_urls)]
    # Convert 'url' column to list
    urls = df_links['url'].astype(str).tolist()
    print(f"Total URLs to scrape: {len(urls)}")
    # Prepare threading
    url_queue = queue.Queue()
    for url in urls:
        url_queue.put(url)
    result_queue = queue.Queue()
    # Initialize locks for CSV files
    success_csv_lock = threading.Lock()
    failed_csv_lock = threading.Lock()
    # Open CSV files in append mode
    success_file_exists = os.path.exists(success_csv_file)
    failed_file_exists = os.path.exists(failed_csv_file)
    success_csv = open(success_csv_file, 'a', newline='', encoding='utf-8')
    failed_csv = open(failed_csv_file, 'a', newline='', encoding='utf-8')
    success_writer = csv.DictWriter(success_csv, fieldnames=success_csv_fields)
    failed_writer = csv.DictWriter(failed_csv, fieldnames=failed_csv_fields)
    # Write headers if files are empty
    if not success_file_exists or os.stat(success_csv_file).st_size == 0:
        success_writer.writeheader()
    if not failed_file_exists or os.stat(failed_csv_file).st_size == 0:
        failed_writer.writeheader()
    # Start worker threads
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=scrape_article_content, args=(url_queue, result_queue, extractor_module))
        t.start()
        threads.append(t)
    # Process results
    total_urls = len(urls)
    processed_urls = 0
    while processed_urls < total_urls:
        try:
            result_type, data = result_queue.get(timeout=60)
            if result_type == 'success':
                # Process columns separately if needed
                # Remove duplicates based on 'url' column (optional here)
                # Write to CSV
                with success_csv_lock:
                    row = {field: data.get(field, '') for field in success_csv_fields}
                    success_writer.writerow(row)
                    success_csv.flush()
                processed_urls += 1
            elif result_type == 'failed':
                # Write to CSV
                with failed_csv_lock:
                    row = {field: data.get(field, '') for field in failed_csv_fields}
                    failed_writer.writerow(row)
                    failed_csv.flush()
                processed_urls += 1
        except queue.Empty:
            break
    # Wait for all threads to finish
    for t in threads:
        t.join()
    # Close CSV files
    success_csv.close()
    failed_csv.close()
    print("Scraping completed.")

if __name__ == "__main__":
    main()
