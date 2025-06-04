# main_scraper.py

import csv
import threading
import queue
import os
import sys
import time
import pandas as pd
import signal
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from importlib import import_module

# Desired request rate (requests per second)
DESIRED_REQUEST_RATE = 16  # Adjust this value as needed

# Maximum number of scraper threads
MAX_THREADS = 24  # Define the number of worker threads

# Time window for statistics (seconds)
STATS_TIME_WINDOW = 10

# Initialize print queue
print_queue = queue.Queue()

RATE_LIMIT_WAIT = 60  # Time to wait upon rate limit detection

# Removed: offset = 0  # Global variable to manage pause state

# Selenium WebDriver options
def get_selenium_options():
    options = Options()
    options.set_preference("permissions.default.image", 2)
    options.set_preference("javascript.enabled", False)
    options.set_preference(
        "dom.ipc.plugins.enabled.libflashplayer.so", False
    )
    options.add_argument("-headless")  # Run in headless mode
    return options

# Stats Tracker class
class StatsTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._success_times = []
        self._fail_times = []
        self._request_times = []
        self.cumulative_success = 0  # ADDED
        self.cumulative_fail = 0     # ADDED

    def record_success(self):
        with self._lock:
            now = time.time()
            self._success_times.append(now)
            self._request_times.append(now)
            self.cumulative_success += 1  # ADDED

    def record_fail(self):
        with self._lock:
            now = time.time()
            self._fail_times.append(now)
            self._request_times.append(now)
            self.cumulative_fail += 1     # ADDED

    def get_stats(self):
        with self._lock:
            current_time = time.time()
            # Remove old times
            self._success_times = [
                t
                for t in self._success_times
                if current_time - t <= STATS_TIME_WINDOW
            ]
            self._fail_times = [
                t
                for t in self._fail_times
                if current_time - t <= STATS_TIME_WINDOW
            ]
            success_count = len(self._success_times)
            fail_count = len(self._fail_times)
            return success_count, fail_count

    def get_actual_rate(self):
        with self._lock:
            current_time = time.time()
            # Remove old request times
            self._request_times = [
                t
                for t in self._request_times
                if current_time - t <= STATS_TIME_WINDOW
            ]
            if self._request_times:
                actual_rate = len(self._request_times) / (
                    current_time - self._request_times[0]
                )
            else:
                actual_rate = 0
            return actual_rate

    def get_cumulative_stats(self):  # ADDED
        with self._lock:
            return self.cumulative_success, self.cumulative_fail  # ADDED

def prGreen(skk, print_queue):
    message = "\033[92m{}\033[00m".format(skk)
    print_queue.put((message, False))

def prRed(skk, print_queue):
    message = "\033[91m{}\033[00m".format(skk)
    print_queue.put((message, False))

# Scraper Thread class
class ScraperThread(threading.Thread):
    def __init__(
        self,
        url_queue,
        result_queue,
        extractor_module,
        stats_tracker,
        stop_event,
        print_queue,
    ):
        super().__init__()
        self.url_queue = url_queue
        self.result_queue = result_queue
        self.extractor_module = extractor_module
        self.stats_tracker = stats_tracker
        self.stop_event = stop_event
        self.print_queue = print_queue

    def run(self):
        # Removed: global offset
        options = get_selenium_options()
        service = Service(executable_path="geckodriver")  # Use absolute path if necessary
        try:
            driver = webdriver.Firefox(service=service, options=options)
            driver.set_page_load_timeout(30)
        except Exception as e:
            prRed(f"Failed to start WebDriver: {e}", self.print_queue)
            self.stop_event.set()
            return
        while not self.stop_event.is_set():
            try:
                url = self.url_queue.get(timeout=1)
            except queue.Empty:
                continue
            try:
                driver.get(url)
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script("return document.readyState") == "complete"
                )

                page_source = driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")
                # Extract data using the extractor module
                data = self.extractor_module.extract_article_data(soup)
                error = data.get("error", "")
                print(error)
                if "rate_limit_reached" in error.lower():  # CHANGED: Detect rate limit based on error
                    prRed("\n\n\n!!!RATE LIMIT DETECTED!!!", self.print_queue)
                    self.result_queue.put(("rate_limit", None))  # ADDED: Send rate_limit signal
                    # Removed: offset = RATE_LIMIT_WAIT
                    continue  # Skip to the next URL or handle accordingly

                # Check if the title is empty
                title = data.get("title", "")
                
                if not title:
                    error_message = "Title is empty"
                    data = {"url": url, "error": error_message}
                    self.result_queue.put(("failed", data))
                    prRed(f"FAIL {url} : {error_message}", self.print_queue)
                    self.stats_tracker.record_fail()
                    continue  # Skip to the next URL

                data["url"] = url
                # Put the result into the result queue
                self.result_queue.put(("success", data))
                prGreen(f"SUCCESS: {url}", self.print_queue)
                self.stats_tracker.record_success()
            except Exception as e:
                error_message = str(e)
                data = {"url": url, "error": error_message}
                self.result_queue.put(("failed", data))
                prRed(f"FAIL {url} : {error_message}", self.print_queue)
                self.stats_tracker.record_fail()
            finally:
                self.url_queue.task_done()
        try:
            driver.quit()
        except PermissionError as e:
            prRed(f"Error terminating WebDriver: {e}", self.print_queue)

# URL Feeder Thread class
class URLFeederThread(threading.Thread):
    def __init__(self, urls, url_queue, stop_event, pause_event, print_queue):
        super().__init__()
        self.urls = urls
        self.url_queue = url_queue
        self.stop_event = stop_event
        self.pause_event = pause_event
        self.print_queue = print_queue
        self.index = 0

    def run(self):
        interval = 1 / DESIRED_REQUEST_RATE  # Time between requests
        while self.index < len(self.urls) and not self.stop_event.is_set():
            if self.pause_event.is_set():  # Check for pause
                self.print_queue.put((f"Pausing for {RATE_LIMIT_WAIT} seconds...", False))
                for i in range(RATE_LIMIT_WAIT, 0, -1):
                    self.print_queue.put((f"Resuming in {i} seconds...", False))
                    time.sleep(1)
                self.pause_event.clear()  # Clear the pause event after waiting
                prGreen("Resuming scraping.", self.print_queue)
            url = self.urls[self.index]
            self.url_queue.put(url)
            self.index += 1
            time.sleep(interval)
        # Signal that no more URLs will be added
        self.stop_event.set()

# Function to display stats
def display_stats(
    stats_tracker, initial_total, already_scraped_success, already_scraped_fails, stop_event, scraper_threads
):
    try:
        while not stop_event.is_set():
            actual_rate = stats_tracker.get_actual_rate()
            success_count, fail_count = stats_tracker.get_stats()
            cumulative_success, cumulative_fail = stats_tracker.get_cumulative_stats()  # ADDED
            total_scraped = cumulative_success + cumulative_fail + already_scraped_success + already_scraped_fails  # ADDED
            progress = (
                (total_scraped / initial_total) * 100 if initial_total else 0
            )
            num_running_threads = len(scraper_threads)
            stats_line = (
                f"Threads: {num_running_threads}/{MAX_THREADS} | "
                f"Request Rate: {actual_rate:.2f}/s | "
                f"Success (last {STATS_TIME_WINDOW}s): {success_count} | "
                f"Fail (last {STATS_TIME_WINDOW}s): {fail_count} | "
                f"Total Scraped: {total_scraped} | "  # ADDED
                f"Progress: {progress:.2f}%"            # ADDED
            )
            # Put the stats line into the print queue
            print_queue.put((stats_line, True))
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass

# Function to handle printing
def print_thread_func(stop_event):
    last_stats_line = ""
    try:
        while not stop_event.is_set() or not print_queue.empty():
            try:
                message, is_stats_line = print_queue.get(timeout=1)
                if is_stats_line:
                    # Overwrite the last stats line
                    sys.stdout.write("\r")  # Move cursor to start of line
                    sys.stdout.write("\033[K")  # Clear line
                    sys.stdout.write(message)
                    sys.stdout.flush()
                    last_stats_line = message
                else:
                    # Print the message above the stats line
                    if last_stats_line:
                        # Move cursor up to insert message
                        sys.stdout.write("\r")  # Move to start of line
                        sys.stdout.write("\033[K")  # Clear line
                        sys.stdout.write(message + "\n")
                        # Reprint the stats line
                        sys.stdout.write(last_stats_line)
                    else:
                        # No stats line yet
                        sys.stdout.write(message + "\n")
                    sys.stdout.flush()
                print_queue.task_done()
            except queue.Empty:
                continue
    except KeyboardInterrupt:
        pass

def main():
    # Handle Ctrl+C gracefully
    def signal_handler(sig, frame):
        print("\nExiting gracefully...")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    website = "yfin"

    # Dynamically import the extractor module
    try:
        extractor_module = import_module(f"extractors.{website}")
    except ImportError:
        print(f"Extractor module for website '{website}' not found.")
        sys.exit(1)

    # Read the CSV file to get the URLs
    input_csv_file = "yahoo_links_new.csv"
    if not os.path.exists(input_csv_file):
        print(f"Input CSV file '{input_csv_file}' not found.")
        sys.exit(1)

    df_links = pd.read_csv(input_csv_file)
    all_urls = df_links["url"].astype(str).tolist()
    initial_total = len(all_urls)  # Total before dropping any  # ADDED

    # Read already scraped URLs from success and failed CSV files
    scraped_urls = set()
    success_csv_file = f"success_articles_{website}.csv"
    failed_csv_file = f"failed_{website}.csv"
    success_csv_fields = [
        "url",
        "datetime",
        "ticker_symbols",
        "author",
        "source",
        "source_url",
        "title",
        "article",
    ]
    failed_csv_fields = ["url", "error"]
    if os.path.exists(success_csv_file):
        df_success_existing = pd.read_csv(success_csv_file)
        scraped_success = len(df_success_existing)
        scraped_urls.update(df_success_existing["url"].astype(str).tolist())
    else:
        df_success_existing = pd.DataFrame(columns=success_csv_fields)
        scraped_success = 0  # ADDED

    if os.path.exists(failed_csv_file):
        df_failed_existing = pd.read_csv(failed_csv_file)
        scraped_fails = len(df_failed_existing)
        scraped_urls.update(df_failed_existing["url"].astype(str).tolist())
    else:
        df_failed_existing = pd.DataFrame(columns=failed_csv_fields)
        scraped_fails = 0  # ADDED

    already_scraped_success = scraped_success  # ADDED
    already_scraped_fails = scraped_fails      # ADDED
    already_scraped_total = already_scraped_success + already_scraped_fails  # ADDED

    # Filter out already scraped URLs
    df_links = df_links[
        ~df_links["url"].astype(str).isin(scraped_urls)
    ]
    # Convert 'url' column to list
    urls = df_links["url"].astype(str).tolist()
    total_urls = len(urls)
    print(f"Total URLs in CSV: {initial_total}")  # ADDED
    print(f"Already scraped (Success + Fails): {already_scraped_total}")  # ADDED
    print(f"Remaining URLs to scrape: {total_urls}")

    # Prepare threading
    url_queue = queue.Queue()
    result_queue = queue.Queue()

    # Initialize locks for CSV files
    success_csv_lock = threading.Lock()
    failed_csv_lock = threading.Lock()

    # Open CSV files in append mode
    success_file_exists = os.path.exists(success_csv_file)
    failed_file_exists = os.path.exists(failed_csv_file)
    success_csv = open(
        success_csv_file, "a", newline="", encoding="utf-8"
    )
    failed_csv = open(
        failed_csv_file, "a", newline="", encoding="utf-8"
    )
    success_writer = csv.DictWriter(
        success_csv, fieldnames=success_csv_fields
    )
    failed_writer = csv.DictWriter(
        failed_csv, fieldnames=failed_csv_fields
    )
    # Write headers if files are empty
    if not success_file_exists or os.stat(success_csv_file).st_size == 0:
        success_writer.writeheader()
    if not failed_file_exists or os.stat(failed_csv_file).st_size == 0:
        failed_writer.writeheader()

    # Initialize the stats tracker
    stats_tracker = StatsTracker()

    # Initialize the pause_event
    pause_event = threading.Event()

    # Start the print thread
    stop_event = threading.Event()
    print_thread = threading.Thread(
        target=print_thread_func, args=(stop_event,)
    )
    print_thread.start()

    # Start stats display thread
    scraper_threads = []
    stats_thread = threading.Thread(
        target=display_stats,
        args=(
            stats_tracker,
            initial_total,             # ADDED
            already_scraped_success,   # ADDED
            already_scraped_fails,     # ADDED
            stop_event,
            scraper_threads,
        ),
    )
    stats_thread.start()

    # Start the worker threads
    for _ in range(MAX_THREADS):
        stop_event_worker = threading.Event()
        thread = ScraperThread(
            url_queue,
            result_queue,
            extractor_module,
            stats_tracker,
            stop_event_worker,
            print_queue,
        )
        thread.start()
        scraper_threads.append(thread)

    # Start the URL Feeder Thread
    feeder_stop_event = threading.Event()
    feeder_thread = URLFeederThread(urls, url_queue, feeder_stop_event, pause_event, print_queue)
    feeder_thread.start()

    # Process results
    total_processed = 0
    while total_processed < total_urls:
        try:
            result_type, data = result_queue.get(timeout=60)
            if result_type == "success":
                # Write to CSV
                with success_csv_lock:
                    row = {
                        field: data.get(field, "")
                        for field in success_csv_fields
                    }
                    success_writer.writerow(row)
                    success_csv.flush()
                total_processed += 1
            elif result_type == "failed":
                # Write to CSV
                with failed_csv_lock:
                    row = {
                        field: data.get(field, "")
                        for field in failed_csv_fields
                    }
                    failed_writer.writerow(row)
                    failed_csv.flush()
                total_processed += 1
            elif result_type == "rate_limit":  # ADDED: Handle rate limit explicitly
                # Initiate waiting by setting pause_event if not already set
                if not pause_event.is_set():
                    print("\nRate limit detected. Pausing scraping.")
                    pause_event.set()  # Signal to URL Feeder Thread to pause
        except queue.Empty:
            break
    # Wait for all URLs to be processed
    feeder_stop_event.set()
    feeder_thread.join()
    url_queue.join()

    # Stop all scraper threads
    for thread in scraper_threads:
        thread.stop_event.set()
    for thread in scraper_threads:
        thread.join()

    # Stop the stats display thread
    stop_event.set()
    stats_thread.join()

    # Wait for all messages in the print queue to be printed
    print_queue.join()
    print_thread.join()

    # Close CSV files
    success_csv.close()
    failed_csv.close()
    print("\nScraping completed.")

if __name__ == "__main__":
    main()
