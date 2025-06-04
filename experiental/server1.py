# server.py

import threading
import queue
import os
import sys
import time
import pandas as pd
import csv
import signal
import socket
import json
from importlib import import_module
import logging
from bs4 import BeautifulSoup

HOST = 'localhost'  # Server IP address
PORT = 8000         # Server port

MAX_CLIENTS = 5     # Maximum number of clients

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StatsTracker:
    def __init__(self):
        self._lock = threading.Lock()
        self._request_times = []
        self._response_times = []
        self.total_responses = 0

    def record_request(self):
        with self._lock:
            now = time.time()
            self._request_times.append(now)

    def record_response(self):
        with self._lock:
            now = time.time()
            self._response_times.append(now)
            self.total_responses += 1

    def get_stats(self):
        with self._lock:
            current_time = time.time()
            # Remove old times
            self._request_times = [t for t in self._request_times if current_time - t <= 10]
            self._response_times = [t for t in self._response_times if current_time - t <= 10]
            request_rate = len(self._request_times) / 10
            response_rate = len(self._response_times) / 10
            return request_rate, response_rate

def display_stats(stats_tracker, total_urls, stop_event):
    try:
        while not stop_event.is_set():
            request_rate, response_rate = stats_tracker.get_stats()
            total_processed = stats_tracker.total_responses
            progress = (total_processed / total_urls) * 100 if total_urls else 0
            stats_line = (
                f"Request Rate: {request_rate:.2f}/s | "
                f"Response Rate: {response_rate:.2f}/s | "
                f"Progress: {progress:.2f}%"
            )
            logger.info(stats_line)
            time.sleep(1)
    except KeyboardInterrupt:
        pass

class ClientHandlerThread(threading.Thread):
    def __init__(self, conn, addr, server):
        super().__init__()
        self.conn = conn
        self.addr = addr
        self.server = server
        self.stop_event = threading.Event()
        self.assigned_urls = set()
        self.lock = threading.Lock()

    def return_unprocessed_urls(self):
        with self.lock:
            for url in self.assigned_urls:
                self.server.url_queue.put(url)
            self.assigned_urls.clear()

    def run(self):
        try:
            while not self.stop_event.is_set():
                data = self.conn.recv(4096)
                if not data:
                    break
                messages = data.decode('utf-8').split('\n')
                for message in messages:
                    if not message.strip():
                        continue
                    try:
                        msg = json.loads(message)
                        print(message)
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error from {self.addr}: {e}")
                        continue
                    if msg['type'] == 'request_tasks':
                        num_urls_requested = msg.get('num_urls', 0)
                        urls_assigned = []
                        for _ in range(num_urls_requested):
                            try:
                                url = self.server.url_queue.get_nowait()
                                urls_assigned.append(url)
                                with self.lock:
                                    self.assigned_urls.add(url)
                            except queue.Empty:
                                break
                        task = {'type': 'task_batch', 'urls': urls_assigned}
                        self.conn.sendall((json.dumps(task) + '\n').encode('utf-8'))
                        self.server.stats_tracker.record_request()
                        logger.info(f"Assigned {len(urls_assigned)} tasks to {self.addr}")
                    elif msg['type'] == 'result':
                        url = msg.get('url')
                        html_content = msg.get('html_content')
                        with self.lock:
                            self.assigned_urls.discard(url)
                        self.server.result_queue.put((url, html_content))
                        self.server.stats_tracker.record_response()
                        logger.info(f"Received result for URL: {url} from {self.addr}")
                    elif msg['type'] == 'tasks_completed':
                        # Optional: Handle task completion acknowledgment
                        self.conn.sendall((json.dumps({'type': 'acknowledge_completion'}) + '\n').encode('utf-8'))
                        self.stop_event.set()
                        logger.info(f"Client {self.addr} has completed all tasks.")
                        break
                    else:
                        logger.error(f"Unknown message type from client {self.addr}: {msg}")
        except (ConnectionResetError, BrokenPipeError) as e:
            logger.warning(f"Client {self.addr} disconnected unexpectedly: {e}")
        except Exception as e:
            logger.error(f"Error with client {self.addr}: {e}")
        finally:
            self.return_unprocessed_urls()
            self.conn.close()
            logger.info(f"Client {self.addr} disconnected.")

class Server:
    def __init__(self):
        self.clients = []
        self.url_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.stats_tracker = StatsTracker()
        self.stats_lock = threading.Lock()
        self.total_urls = 0
        self.stop_event = threading.Event()

    def load_urls(self):
        website = "yfin"

        # Read the CSV file to get the URLs
        input_csv_file = "yahoo_links_new.csv"
        if not os.path.exists(input_csv_file):
            logger.error(f"Input CSV file '{input_csv_file}' not found.")
            sys.exit(1)

        df_links = pd.read_csv(input_csv_file)
        # Read already scraped URLs from success and failed CSV files
        scraped_urls = set()
        success_csv_file = f"success_articles_{website}.csv"
        failed_csv_file = f"failed_articles_{website}.csv"
        if os.path.exists(success_csv_file):
            df_success_existing = pd.read_csv(success_csv_file)
            scraped_urls.update(df_success_existing["url"].astype(str).tolist())
        if os.path.exists(failed_csv_file):
            df_failed_existing = pd.read_csv(failed_csv_file)
            scraped_urls.update(df_failed_existing["url"].astype(str).tolist())
        # Filter out already scraped URLs
        df_links = df_links[~df_links["url"].astype(str).isin(scraped_urls)]
        # Convert 'url' column to list
        urls = df_links["url"].astype(str).tolist()
        self.total_urls = len(urls)
        logger.info(f"Total URLs to scrape: {self.total_urls}")
        for url in urls:
            self.url_queue.put(url)

    def start(self):
        # Handle Ctrl+C gracefully
        def signal_handler(sig, frame):
            logger.info("Exiting gracefully...")
            self.stop_event.set()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        # Load URLs
        self.load_urls()

        # Start stats display thread
        stats_thread = threading.Thread(
            target=display_stats,
            args=(self.stats_tracker, self.total_urls, self.stop_event),
            daemon=True
        )
        stats_thread.start()

        # Start server socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((HOST, PORT))
            s.listen(MAX_CLIENTS)
            s.settimeout(1.0)  # To allow periodic checks for stop_event
            logger.info(f"Server listening on {HOST}:{PORT}")
            while not self.stop_event.is_set():
                try:
                    conn, addr = s.accept()
                    logger.info(f"Connected by {addr}")
                    client_thread = ClientHandlerThread(conn, addr, self)
                    client_thread.start()
                    self.clients.append(client_thread)
                except socket.timeout:
                    continue
                except Exception as e:
                    logger.error(f"Error accepting connections: {e}")
                    break

        # Wait for all clients to finish
        for client in self.clients:
            client.stop_event.set()
            client.join()

        # Process results
        self.process_results()

        # Stop threads
        self.stop_event.set()
        stats_thread.join()

    def process_results(self):
        website = "yfin"
        try:
            extractor_module = import_module(f"extractors.{website}")
        except ImportError:
            logger.error(f"Extractor module for website '{website}' not found.")
            sys.exit(1)
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

        # Open CSV files in append mode
        success_csv_lock = threading.Lock()
        failed_csv_lock = threading.Lock()
        success_file_exists = os.path.exists(success_csv_file)
        failed_file_exists = os.path.exists(failed_csv_file)
        success_csv = open(success_csv_file, "a", newline="", encoding="utf-8")
        failed_csv = open(failed_csv_file, "a", newline="", encoding="utf-8")
        success_writer = csv.DictWriter(success_csv, fieldnames=success_csv_fields)
        failed_writer = csv.DictWriter(failed_csv, fieldnames=failed_csv_fields)
        # Write headers if files are empty
        if not success_file_exists or os.stat(success_csv_file).st_size == 0:
            success_writer.writeheader()
        if not failed_file_exists or os.stat(failed_csv_file).st_size == 0:
            failed_writer.writeheader()

        # Process results from result_queue
        while not self.result_queue.empty():
            url, html_content = self.result_queue.get()
            try:
                if html_content.startswith("ERROR:"):
                    # This is an error from the client
                    error_message = html_content[6:]
                    data = {"url": url, "error": error_message}
                    with failed_csv_lock:
                        row = {field: data.get(field, "") for field in failed_csv_fields}
                        failed_writer.writerow(row)
                        failed_csv.flush()
                    logger.error(f"Failed to scrape {url}: {error_message}")
                    continue

                soup = BeautifulSoup(html_content, "html.parser")
                data = extractor_module.extract_article_data(soup)
                title = data.get("title", "")
                if not title:
                    error_message = "Title is empty"
                    data = {"url": url, "error": error_message}
                    with failed_csv_lock:
                        row = {field: data.get(field, "") for field in failed_csv_fields}
                        failed_writer.writerow(row)
                        failed_csv.flush()
                    logger.error(f"Failed to scrape {url}: {error_message}")
                    continue

                data["url"] = url
                with success_csv_lock:
                    row = {field: data.get(field, "") for field in success_csv_fields}
                    success_writer.writerow(row)
                    success_csv.flush()
                logger.info(f"Successfully scraped {url}")
            except Exception as e:
                error_message = str(e)
                data = {"url": url, "error": error_message}
                with failed_csv_lock:
                    row = {field: data.get(field, "") for field in failed_csv_fields}
                    failed_writer.writerow(row)
                    failed_csv.flush()
                logger.error(f"Error processing result for {url}: {error_message}")

        # Close CSV files
        success_csv.close()
        failed_csv.close()

if __name__ == "__main__":
    server = Server()
    server.start()
