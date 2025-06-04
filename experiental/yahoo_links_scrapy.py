import scrapy
from scrapy.crawler import CrawlerProcess
from bs4 import BeautifulSoup
import os

string = ''
class MySpider(scrapy.Spider):
    name = 'myspider'
    custom_settings = {
        'USER_AGENT': 'Mozilla/5.0 (compatible; MyBot/0.1)',
        'FEEDS': {
            'yahoo_links_new_raw.txt': {
                'format': 'txt',
                'store_empty': False,
            },
        },
        'DOWNLOAD_WARNSIZE':0
    }

    def start_requests(self):
        # Define the list of URLs to scrape here
        char_list = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z','1','2','3','4','5','6','7','8','9','0','-','_','$']
        scraped = os.listdir('yahoo_links_1')
        for ch0 in char_list:
            for ch1 in char_list:
                if not f'yahoo_{ch0}{ch1}.txt' in scraped:
                    url = f'http://web.archive.org/cdx/search/?url=https://www.finance.yahoo.com/news/{ch0}{ch1}*'
                    yield scrapy.Request(url=url, callback=self.parse)
        

    def parse(self, response):
        body = response.body.decode('utf-8')
        # Use BeautifulSoup to parse the response
        soup = BeautifulSoup(body, 'html.parser')
        # paragraphs = soup.find_all('p')

        # Extract data with BeautifulSoup
        text = soup.get_text(separator='\n', strip=True)
        #print(text)
        print('#####################')
        filename = 'yahoo_'+response.url.split('news/')[-1].split('*')[0]+'.txt'
        with open('yahoo_links_1/'+filename, 'w') as file:
            file.write(text)
        # for p in paragraphs:
        #     print(p.text)
        # You can also use Scrapy's built-in selectors instead of BeautifulSoup
        # For example, to extract all text from a page:
        # text = ' '.join(response.xpath('//text()').extract())
        # print(text)

        # Here you would define your logic to process the extracted data,
        # save it to a file, store it in a database, or anything else

# Main script to start the crawler
def run_spider():
    process = CrawlerProcess()

    process.crawl(MySpider)
    process.start()  # the script will block here until the crawling is finished

# Run the spider
run_spider()
