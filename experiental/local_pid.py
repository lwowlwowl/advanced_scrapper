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
DESIRED_REQUEST_RATE = 7  # Adjust this value as needed

# Maximum number of scraper threads
MAX_THREADS = 12  # Define an upper limit for safety

# Time window for statistics (seconds)
STATS_TIME_WINDOW = 5

# Initialize print queue
print_queue = queue.Queue()

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

# PID Controller class with different gains for acceleration and deceleration
class PIDController:
    def __init__(self, setpoint, Kp_accel, Ki_accel, Kd_accel, Kp_decel, Ki_decel, Kd_decel):
        self.setpoint = setpoint  # Desired request rate
        # Gains for acceleration (increasing request rate)
        self.Kp_accel = Kp_accel
        self.Ki_accel = Ki_accel
        self.Kd_accel = Kd_accel
        # Gains for deceleration (decreasing request rate)
        self.Kp_decel = Kp_decel
        self.Ki_decel = Ki_decel
        self.Kd_decel = Kd_decel
        self._lock = threading.Lock()
        self._last_time = None
        self._last_error = 0
        self._integral = 0

    def compute(self, actual_rate):
        with self._lock:
            current_time = time.time()
            error = self.setpoint - actual_rate
            delta_time = current_time - self._last_time if self._last_time else 0
            delta_error = error - self._last_error

            if error >= 0:
                # Acceleration gains
                Kp = self.Kp_accel
                Ki = self.Ki_accel
                Kd = self.Kd_accel
            else:
                # Deceleration gains
                Kp = self.Kp_decel
                Ki = self.Ki_decel
                Kd = self.Kd_decel

            if delta_time > 0:
                derivative = delta_error / delta_time
            else:
                derivative = 0

            self._integral += error * delta_time

            output = Kp * error + Ki * self._integral + Kd * derivative

            # Update state
            self._last_time = current_time
            self._last_error = error

            return output

# Stats Tracker class
class StatsTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._success_times = []
        self._fail_times = []
        self._request_times = []

    def record_success(self):
        with self._lock:
            now = time.time()
            self._success_times.append(now)
            self._request_times.append(now)

    def record_fail(self):
        with self._lock:
            now = time.time()
            self._fail_times.append(now)
            self._request_times.append(now)

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
        options = get_selenium_options()
        service = Service(executable_path="geckodriver")
        driver = webdriver.Firefox(service=service, options=options)
        driver.set_page_load_timeout(30)
        while not self.stop_event.is_set():
            try:
                url = self.url_queue.get_nowait()
            except queue.Empty:
                break
            try:
                driver.get(url)
                WebDriverWait(driver, 10).until(
                    lambda d: d.execute_script("return document.readyState")
                    == "complete"
                )

                page_source = driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")
                # Extract data using the extractor module
                data = self.extractor_module.extract_article_data(soup)

                # Check if the title is empty
                title = data.get("title", "")
                if not title:
                    error_message = "Title is empty"
                    data = {"url": url, "error": error_message}
                    self.result_queue.put(("failed", data))
                    prRed(
                        f"FAIL {url} : {error_message}", self.print_queue
                    )
                    self.stats_tracker.record_fail()
                    continue  # Skip to the next URL

                data["url"] = url
                # Put the result into the result queue
                self.result_queue.put(("success", data))
                prGreen(f"success: {url}", self.print_queue)
                self.stats_tracker.record_success()
            except Exception as e:
                error_message = str(e)
                data = {"url": url, "error": error_message}
                self.result_queue.put(("failed", data))
                prRed(f"FAIL {url} : {error_message}", self.print_queue)
                self.stats_tracker.record_fail()
            finally:
                self.url_queue.task_done()
        driver.quit()

# Monitoring Thread class
class MonitoringThread(threading.Thread):
    def __init__(
        self,
        pid_controller,
        stats_tracker,
        scraper_threads,
        scraper_threads_lock,
        url_queue,
        result_queue,
        extractor_module,
        print_queue,
    ):
        super().__init__()
        self.pid_controller = pid_controller
        self.stats_tracker = stats_tracker
        self.scraper_threads = scraper_threads
        self.scraper_threads_lock = scraper_threads_lock
        self.url_queue = url_queue
        self.result_queue = result_queue
        self.extractor_module = extractor_module
        self.print_queue = print_queue
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            actual_rate = self.stats_tracker.get_actual_rate()
            output = self.pid_controller.compute(actual_rate)

            with self.scraper_threads_lock:
                current_thread_count = len(self.scraper_threads)
                # Determine desired thread count
                desired_thread_count = current_thread_count + int(output)
                desired_thread_count = max(1, min(desired_thread_count, MAX_THREADS))
                delta_threads = desired_thread_count - current_thread_count

                if delta_threads > 0:
                    # Add threads
                    for _ in range(delta_threads):
                        stop_event = threading.Event()
                        thread = ScraperThread(
                            self.url_queue,
                            self.result_queue,
                            self.extractor_module,
                            self.stats_tracker,
                            stop_event,
                            self.print_queue,
                        )
                        thread.start()
                        self.scraper_threads.append((thread, stop_event))
                elif delta_threads < 0:
                    # Remove threads
                    for _ in range(-delta_threads):
                        if self.scraper_threads:
                            thread, stop_event = self.scraper_threads.pop()
                            stop_event.set()
                            thread.join()
            time.sleep(0.8)

# Function to display stats
def display_stats(
    stats_tracker, total_urls, stop_event, scraper_threads, scraper_threads_lock
):
    try:
        while not stop_event.is_set():
            actual_rate = stats_tracker.get_actual_rate()
            success_count, fail_count = stats_tracker.get_stats()
            total_processed = success_count + fail_count
            progress = (
                (total_processed / total_urls) * 100 if total_urls else 0
            )
            with scraper_threads_lock:
                num_running_threads = len(scraper_threads)
            stats_line = (
                f"Threads: {num_running_threads}/{MAX_THREADS} | "
                f"Request Rate: {actual_rate:.2f}/s | "
                f"Success (last {STATS_TIME_WINDOW}s): {success_count} | "
                f"Fail (last {STATS_TIME_WINDOW}s): {fail_count} | "
                f"Progress: {progress:.2f}%"
            )
            # Put the stats line into the print queue
            print_queue.put((stats_line, True))
            time.sleep(0.5)
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
    # Read already scraped URLs from success and failed CSV files
    scraped_urls = set()
    success_csv_file = f"success_articles_{website}.csv"
    failed_csv_file = f"failed_articles_{website}.csv"
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
        scraped_urls.update(df_success_existing["url"].astype(str).tolist())
    else:
        df_success_existing = pd.DataFrame(columns=success_csv_fields)
    if os.path.exists(failed_csv_file):
        df_failed_existing = pd.read_csv(failed_csv_file)
        scraped_urls.update(df_failed_existing["url"].astype(str).tolist())
    else:
        df_failed_existing = pd.DataFrame(columns=failed_csv_fields)
    # Filter out already scraped URLs
    df_links = df_links[
        ~df_links["url"].astype(str).isin(scraped_urls)
    ]
    # Convert 'url' column to list
    urls = df_links["url"].astype(str).tolist()
    total_urls = len(urls)
    print(f"Total URLs to scrape: {total_urls}")
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
    # Initialize the scraper_threads list and lock
    scraper_threads = []
    scraper_threads_lock = threading.Lock()
    # Initialize the PID controller with different gains for acceleration and deceleration
    pid_controller = PIDController(
        setpoint=DESIRED_REQUEST_RATE,
        Kp_accel=0.5, Ki_accel=0.000, Kd_accel=0.0000,  # Gains for acceleration
        Kp_decel=1.0, Ki_decel=0.00, Kd_decel=0.00,   # Gains for deceleration
    )
    # Start the print thread
    stop_event = threading.Event()
    print_thread = threading.Thread(
        target=print_thread_func, args=(stop_event,)
    )
    print_thread.start()
    # Start stats display thread
    stats_thread = threading.Thread(
        target=display_stats,
        args=(
            stats_tracker,
            total_urls,
            stop_event,
            scraper_threads,
            scraper_threads_lock,
        ),
    )
    stats_thread.start()
    # Start the MonitoringThread
    monitoring_thread = MonitoringThread(
        pid_controller,
        stats_tracker,
        scraper_threads,
        scraper_threads_lock,
        url_queue,
        result_queue,
        extractor_module,
        print_queue,
    )
    monitoring_thread.start()
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
        except queue.Empty:
            break
    # Wait for all threads to finish
    monitoring_thread.stop_event.set()
    monitoring_thread.join()
    # Stop all scraper threads
    with scraper_threads_lock:
        for thread, stop_event in scraper_threads:
            stop_event.set()
    with scraper_threads_lock:
        for thread, _ in scraper_threads:
            thread.join()
    # Stop the stats display thread
    stop_event.set()
    stats_thread.join()
    print_queue.join()
    print_thread.join()
    # Close CSV files
    success_csv.close()
    failed_csv.close()
    print("\nScraping completed.")

if __name__ == "__main__":
    main()
