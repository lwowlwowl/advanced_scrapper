from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import pika
import time

def scrape(url):
    # Configure Selenium WebDriver
    options = Options()
    options.headless = True  # Run in headless mode
    service = Service('path/to/chromedriver')  # Specify path to chromedriver

    # Start the WebDriver
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)

    # Perform your scraping tasks here...
    # For example, get the page title
    page_title = driver.title
    print(f"Scraped title from {url}: {page_title}")

    # Close the WebDriver
    driver.quit()

    # You can return results here if needed
    return page_title

def callback(ch, method, properties, body):
    url = body.decode()
    print(f" [x] Received {url}")

    # Perform the scraping
    try:
        result = scrape(url)
        # Send acknowledgment after task completion
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print(f"Failed to scrape {url}: {e}")
        # Optionally, send a negative acknowledgment
        ch.basic_nack(delivery_tag=method.delivery_tag)

# Establish a connection to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare the same queue as the producer to ensure it exists
queue_name = 'scraping_tasks'
channel.queue_declare(queue=queue_name)

# Start consuming messages from the queue
channel.basic_qos(prefetch_count=1)  # Optional: allows fair dispatch among workers
channel.basic_consume(queue=queue_name, on_message_callback=callback)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()