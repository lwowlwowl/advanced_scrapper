import pika

# Establish a connection to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare a queue to send messages to
queue_name = 'scraping_tasks'
channel.queue_declare(queue=queue_name)

# Function to publish a message to the queue
def publish_scraping_task(url):
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=url)
    print(f" [x] Sent '{url}'")

# Example usage
publish_scraping_task('http://example.com/page1')
publish_scraping_task('http://example.com/page2')

# Close the connection
connection.close()