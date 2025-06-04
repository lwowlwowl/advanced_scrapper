import csv
import json
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
import time
import os
import pandas as pd
import threading
import queue
import traceback
import random
import re
import logging

# Number of worker threads
num_threads = 16

# Selenium WebDriver options
def get_selenium_options():
    options = Options()
    options.set_preference("permissions.default.image", 2)
    options.set_preference("javascript.enabled", False)
    options.set_preference("dom.ipc.plugins.enabled.libflashplayer.so", False)
    # options.add_argument("-headless")  # Run in headless mode
    return options

# Template for scraping
template = {
    'title': 'h1.cover-title',
    'author': 'div.byline-attr-author',
    'datetime': {'selector': 'time', 'attribute': 'datetime', 'index': [0]},
    'article': 'div.body',
    'ticker_symbols': 'span.symbol',
    'source': 'a[class="subtle-link fin-size-small"]',
    'source_url': {'selector': 'a[class="link caas-attr-provider-logo"]', 'attribute': 'href', 'index': [0]},
}

# Function to extract ticker symbols from <a> tags
def extract_ticker_symbols_from_links(soup):
    ticker_symbols = set()
    # Find all <a> tags within the article text section
    article_section = soup.select_one('div.caas-body')
    if article_section:
        links = article_section.find_all('a', href=True)
        for link in links:
            href = link['href']
            # Check if the href contains the quote subdomain
            match = re.search(r'https://finance\.yahoo\.com/quote/([A-Za-z0-9\-.]+)', href)
            if match:
                ticker_symbol = match.group(1)
                ticker_symbols.add(ticker_symbol)
    return list(ticker_symbols)

# Function to extract article data
def extract_article_data(soup, template):
    article_data = {}
    
    def extract_elements(selector_info, parent_element):
        selector = selector_info['selector']
        attribute = selector_info.get('attribute', 'text')
        index = selector_info.get('index')
        inner = selector_info.get('inner')

        elements = parent_element.select(selector)
        if elements:
            if index:
                elements = [elements[i] for i in index if i < len(elements)]
            value = []
            for element in elements:
                if inner:
                    inner_value = extract_elements(inner, element)
                    value.append(inner_value)
                else:
                    if attribute == 'text':
                        value.append(element.get_text(strip=True))
                    else:
                        value.append(element.get(attribute, ''))
            return value
        else:
            return []
    
    for field, selector_info in template.items():
        try:
            if isinstance(selector_info, dict):
                value = extract_elements(selector_info, soup)
                article_data[field] = value
            else:
                element = soup.select_one(selector_info)
                if element:
                    article_data[field] = element.get_text(strip=True)
                else:
                    article_data[field] = ''
        except Exception as e:
            logging.error(f"Error extracting {field}: {e}")
            article_data[field] = ''
    return article_data

# Function to scrape article content
def scrape_article_content(url_queue, result_queue):
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
            # Extract data using the template
            data = extract_article_data(soup, template)

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
                print(soup)
                print(f"Failed to scrape {filename}: {error_message}")
                continue  # Skip to the next URL

            # Extract ticker symbols from <a> tags
            ticker_symbols_links = extract_ticker_symbols_from_links(soup)
            # Combine ticker symbols
            ticker_symbols = set(data.get('ticker_symbols', []))
            ticker_symbols.update(ticker_symbols_links)
            data['ticker_symbols'] = list(ticker_symbols)
            data['url'] = url
            # Put the result into the result queue
            result_queue.put(('success', data))
            print(f"Scraped and saved: {filename}")
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

# Main function
if __name__ == "__main__":
    # Read the CSV file to get the URLs
    df_links = pd.read_csv('yahoo_links_new.csv')
    # Read already scraped URLs from success and failed CSV files
    scraped_urls = set()
    success_csv_file = 'success_articles.csv'
    failed_csv_file = 'failed_articles.csv'
    if os.path.exists(success_csv_file):
        df_success = pd.read_csv(success_csv_file)
        scraped_urls.update(df_success['url'].astype(str).tolist())
    if os.path.exists(failed_csv_file):
        df_failed = pd.read_csv(failed_csv_file)
        scraped_urls.update(df_failed['url'].astype(str).tolist())
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
    # Prepare CSV writers
    success_csv_fields = ['url', 'datetime', 'ticker_symbols',  'author', 'source', 'source_url', 'title', 'article', ]
    failed_csv_fields = ['url', 'error']
    # Open CSV files
    success_csv_lock = threading.Lock()
    failed_csv_lock = threading.Lock()
    with open(success_csv_file, 'a', newline='', encoding='utf-8') as success_csv, \
         open(failed_csv_file, 'a', newline='', encoding='utf-8') as failed_csv:
        success_writer = csv.DictWriter(success_csv, fieldnames=success_csv_fields)
        failed_writer = csv.DictWriter(failed_csv, fieldnames=failed_csv_fields)
        # Write headers if files are empty
        if os.stat(success_csv_file).st_size == 0:
            success_writer.writeheader()
        if os.stat(failed_csv_file).st_size == 0:
            failed_writer.writeheader()
        # Start worker threads
        threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=scrape_article_content, args=(url_queue, result_queue))
            t.start()
            threads.append(t)
        # Process results
        total_urls = len(urls)
        processed_urls = 0
        while processed_urls < total_urls:
            try:
                result_type, data = result_queue.get(timeout=60)
                if result_type == 'success':
                    with success_csv_lock:

                        # Ensure that data contains all the fields
                        row = {field: data.get(field, '') for field in success_csv_fields}
                        print(row,type(row))
                        success_writer.writerow(row)
                    processed_urls += 1
                elif result_type == 'failed':
                    with failed_csv_lock:
                        # Ensure that data contains all the fields
                        row = {field: data.get(field, '') for field in failed_csv_fields}
                        failed_writer.writerow(row)
                    processed_urls += 1
            except queue.Empty:
                break
        # Wait for all threads to finish
        for t in threads:
            t.join()
    print("Scraping completed.")
