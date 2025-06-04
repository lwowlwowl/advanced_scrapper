from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options

print("Starting test...")
options = Options()
# options.add_argument("-headless")
service = Service(executable_path='geckodriver')
driver = webdriver.Firefox(service=service, options=options)
print("Browser opened")
driver.get("https://www.finance.yahoo.com")
print("Page title:", driver.title)
driver.quit()
print("Test completed")