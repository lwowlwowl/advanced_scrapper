# Advanced High Speed Universal Scrapper

The currently available program is `constant_rate_scrapper.py`

- Muti-thread Firefox-geckodriver, queueing mechanism
- Send requests at a constant rate, great for avoiding rate limit
- Rate limit detection and pausing mechainsm
- Use template form `extractors` folder (Yahoo Finance as example)
- Log both succeed and failed articles, automatically resume the progress when restart, simple CSV storage

`yahoo_links_selenium.py` is used to get all the recorded Yahoo Finance news links on Internet Archive through its CDX server. It loops through prefix "00*" - "zz*", since on some link prefixes only return limited amount of results because there's too much urls. All the succeed fetches will be cached in the "parts" folder (also capable for automatic resuming after restart). Finally it drops the duplicates and output a CSV file that could feed to the scrapper. 

`experimental` folder holds all the experimental programs, future developmet including distributed system and more advanced with computer vision universal templateless scrapper. 