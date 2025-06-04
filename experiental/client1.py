# client.py

import threading
import queue
import sys
import time
import socket
import json
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait

HOST = 'localhost'  # Server IP address
PORT = 8000         # Server port

# Desired request rate (requests per second)
DESIRED_REQUEST_RATE = 8  # Adjust this value as needed

# Maximum number of scraper threads
MAX_THREADS = 8  # Define the number of worker threads

MIN_QUEUE_LENGTH = 10  # Minimum task queue length before requesting more URLs
BATCH_SIZE = 20        # Number of URLs to request from the server at a time

# Initialize print queue
print_queue = queue.Queue()

# Selenium WebDriver options
def get_selenium_options():
    options = Options()
    options.set_preference("permissions.default.image", 2)
    options.set_preference("javascript.enabled", False)
    options.set_preference("dom.ipc.plugins.enabled.libflashplayer.so", False)
    options.add_argument("-headless")  # Run in headless mode
    return options

# Scraper Thread class
class ScraperThread(threading.Thread):
    def __init__(self, task_queue, result_queue, stop_event):
        super().__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.stop_event = stop_event

    def run(self):
        options = get_selenium_options()
        service = Service(executable_path="geckodriver")
        try:
            driver = webdriver.Firefox(service=service, options=options)
            driver.set_page_load_timeout(30)
        except Exception as e:
            print_queue.put(f"Failed to start WebDriver: {e}")
            self.stop_event.set()
            return
        try:
            while not self.stop_event.is_set():
                try:
                    url = self.task_queue.get(timeout=1)
                except queue.Empty:
                    if self.stop_event.is_set():
                        break
                    else:
                        continue
                try:
                    driver.get(url)
                    WebDriverWait(driver, 10).until(
                        lambda d: d.execute_script("return document.readyState") == "complete"
                    )
                    page_source = driver.page_source
                    # Send result back to server
                    self.result_queue.put((url, page_source))
                except Exception as e:
                    # Send error back to server
                    self.result_queue.put((url, f"ERROR: {e}"))
                finally:
                    self.task_queue.task_done()
        finally:
            try:
                driver.quit()
            except PermissionError as e:
                print_queue.put(f"PermissionError quitting driver: {e}")
            except Exception as e:
                print_queue.put(f"Error quitting driver: {e}")

# Client class
class Client:
    def __init__(self):
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.sock = None
        self.sock_lock = threading.Lock()

    def connect_to_server(self):
        while not self.stop_event.is_set():
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((HOST, PORT))
                print_queue.put("Connected to server.")
                break
            except Exception as e:
                print_queue.put(f"Connection failed: {e}, retrying in 5 seconds...")
                time.sleep(5)

    def start(self):
        # Start print thread
        print_thread = threading.Thread(target=self.print_thread_func, daemon=True)
        print_thread.start()

        # Start worker threads
        self.scraper_threads = []
        for _ in range(MAX_THREADS):
            thread = ScraperThread(self.task_queue, self.result_queue, self.stop_event)
            thread.start()
            self.scraper_threads.append(thread)

        # Start receiving tasks from server
        receiver_thread = threading.Thread(target=self.receive_tasks, daemon=True)
        receiver_thread.start()

        # Start sending results to server
        sender_thread = threading.Thread(target=self.send_results, daemon=True)
        sender_thread.start()

        # Start monitoring task queue length
        monitor_thread = threading.Thread(target=self.monitor_task_queue, daemon=True)
        monitor_thread.start()

        # Keep the main thread alive to handle KeyboardInterrupt
        try:
            while not self.stop_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            print_queue.put("KeyboardInterrupt received. Shutting down...")
            self.stop_event.set()

        # Wait for all threads to finish
        receiver_thread.join()
        sender_thread.join()
        monitor_thread.join()
        for thread in self.scraper_threads:
            thread.join()
        print_thread.join()

    def receive_tasks(self):
        buffer = ""
        try:
            while not self.stop_event.is_set():
                data = self.sock.recv(4096)
                if not data:
                    print_queue.put("Server closed the connection.")
                    self.stop_event.set()
                    break
                buffer += data.decode('utf-8')
                while '\n' in buffer:
                    message, buffer = buffer.split('\n', 1)
                    if not message.strip():
                        continue
                    try:
                        msg = json.loads(message)
                        print(message)
                    except json.JSONDecodeError as e:
                        print_queue.put(f"JSON decode error: {e}")
                        continue
                    if msg['type'] == 'task_batch':
                        urls = msg.get('urls', [])
                        for url in urls:
                            self.task_queue.put(url)
                        print_queue.put(f"Received {len(urls)} new tasks.")
                    elif msg['type'] == 'no_task':
                        print_queue.put("No more tasks available from server.")
                        self.stop_event.set()
                        break
                    else:
                        print_queue.put(f"Unknown message type from server: {msg}")
        except Exception as e:
            print_queue.put(f"Error receiving tasks: {e}")
            self.stop_event.set()
        finally:
            self.sock.close()

    def send_results(self):
        try:
            while not self.stop_event.is_set() or not self.result_queue.empty():
                try:
                    url, result = self.result_queue.get(timeout=1)
                except queue.Empty:
                    continue
                try:
                    message = {'type': 'result', 'url': url, 'html_content': result}
                    with self.sock_lock:
                        self.sock.sendall((json.dumps(message) + '\n').encode('utf-8'))
                    self.result_queue.task_done()
                except BrokenPipeError:
                    print_queue.put("Server disconnected. Cannot send results.")
                    self.stop_event.set()
                    break
                except Exception as e:
                    print_queue.put(f"Error sending results: {e}")
                    self.stop_event.set()
                    break
        except Exception as e:
            print_queue.put(f"Error in send_results: {e}")
            self.stop_event.set()
        finally:
            self.sock.close()

    def monitor_task_queue(self):
        try:
            while not self.stop_event.is_set():
                queue_length = self.task_queue.qsize()
                print_queue.put(f"Task Queue Length: {queue_length}")
                if queue_length < MIN_QUEUE_LENGTH:
                    # Request more tasks from server
                    try:
                        with self.sock_lock:
                            request = {'type': 'request_tasks', 'num_urls': BATCH_SIZE}
                            self.sock.sendall((json.dumps(request) + '\n').encode('utf-8'))
                            print_queue.put(f"Requested {BATCH_SIZE} more tasks from server.")
                    except BrokenPipeError:
                        print_queue.put("Server disconnected. Cannot request more tasks.")
                        self.stop_event.set()
                        break
                    except Exception as e:
                        print_queue.put(f"Error requesting more tasks: {e}")
                        self.stop_event.set()
                        break
                    time.sleep(1 / DESIRED_REQUEST_RATE)  # To maintain desired request rate
                else:
                    time.sleep(1)
        except Exception as e:
            print_queue.put(f"Error monitoring task queue: {e}")
            self.stop_event.set()

    def print_thread_func(self):
        try:
            while not self.stop_event.is_set() or not print_queue.empty():
                try:
                    message = print_queue.get(timeout=1)
                    print(message)
                    sys.stdout.flush()
                    print_queue.task_done()
                except queue.Empty:
                    continue
        except KeyboardInterrupt:
            pass

if __name__ == "__main__":
    client = Client()
    client.connect_to_server()
    client.start()
