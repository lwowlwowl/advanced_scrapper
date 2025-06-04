# extractors/extractor_yahoo.py

import re
import json
from bs4 import BeautifulSoup

def extract_article_data(soup):
    article_data = {}



    # Extract title
    title_element = soup.select_one('div.cover-title')
    if title_element:
        article_data['title'] = title_element.get_text(strip=True)
    else:
        article_data['title'] = ''
        rate_limit = soup.get_text()
        # print(rate_limit)
        if (("Thank you for your patience." in rate_limit) and ("Our engineers are working quickly to resolve the issue." in rate_limit)) or ("Edge: Not Found" in rate_limit):
            article_data['error'] = "rate_limit_reached"

    # Extract author
    author_element = soup.select_one('div.byline-attr-author')
    if author_element:
        article_data['author'] = author_element.get_text(strip=True)
    else:
        article_data['author'] = ''

    # Extract datetime
    datetime_element = soup.find('time')
    if datetime_element and datetime_element.has_attr('datetime'):
        article_data['datetime'] = datetime_element['datetime']
    else:
        article_data['datetime'] = ''

    # Extract article content
    article_element = soup.select_one('div.body')
    if article_element:
        # Initialize an empty list to hold the text segments
        content_parts = []

        # Function to process the elements recursively
        def process_element(element):
            # If the element is a paragraph <p>, extract the text
            if element.name == 'p':
                text = element.get_text(strip=True)
                if text:
                    content_parts.append(text)
            # If the element is an unordered list <ul> or ordered list <ol>
            elif element.name in ['ul', 'ol']:
                is_ordered = element.name == 'ol'
                list_items = element.find_all('li', recursive=False)
                for idx, li in enumerate(list_items, 1):
                    li_text = li.get_text(strip=True)
                    if li_text:
                        if is_ordered:
                            content_parts.append(f'{idx}. {li_text}')
                        else:
                            content_parts.append(f'• {li_text}')
            # If the element is a list item <li> outside of a list (edge case)
            elif element.name == 'li':
                li_text = element.get_text(strip=True)
                if li_text:
                    content_parts.append(f'• {li_text}')
            # If the element is a table
            elif element.name == 'table':
                # Convert the table to JSON
                table_json = convert_table_to_json(element)
                if table_json:
                    content_parts.append(table_json)
            else:
                # For other elements, process their children
                for child in element.contents:
                    if isinstance(child, str):
                        continue  # Skip strings outside of desired tags
                    else:
                        process_element(child)

        # Function to convert table to JSON
        def convert_table_to_json(table_element):
            # Extract table headers and rows
            table_data = []
            headers = []

            # Find all rows in the table
            rows = table_element.find_all('tr')
            if not rows:
                return None  # Return None if there are no rows

            # Assume the first row might be headers
            first_row = rows[0]
            header_cells = first_row.find_all(['th', 'td'])
            headers = [cell.get_text(strip=True) for cell in header_cells]

            # Check if headers are meaningful (not all empty)
            if any(headers):
                data_rows = rows[1:]  # Exclude header row
            else:
                # No meaningful headers, treat all rows as data
                headers = []
                data_rows = rows

            # Extract data from rows
            for row in data_rows:
                cells = row.find_all(['th', 'td'])
                cell_data = [cell.get_text(strip=True) for cell in cells]
                if headers and len(headers) == len(cell_data):
                    row_data = dict(zip(headers, cell_data))
                else:
                    row_data = cell_data
                table_data.append(row_data)

            # Convert the table data to JSON
            table_json = json.dumps(table_data)
            return table_json

        # Start processing from the article element
        process_element(article_element)

        # Join the content parts with newlines
        article_text = '\n'.join(content_parts)
        article_data['article'] = article_text
    else:
        article_data['article'] = ''

    # Extract ticker symbols from links in the article
    ticker_symbols = set()
    ticker_symbols_links = extract_ticker_symbols_from_links(soup)
    ticker_symbols.update(ticker_symbols_links)
    article_data['ticker_symbols'] = list(ticker_symbols)

    # Extract source
    source_element = soup.select_one('a.subtle-link.fin-size-small')
    if source_element and source_element.has_attr('aria-label'):
        article_data['source'] = source_element['aria-label']
    else:
        article_data['source'] = ''

    # Extract source_url
    source_url_element = soup.select_one('a.subtle-link.fin-size-small')
    if source_url_element and source_url_element.has_attr('href'):
        article_data['source_url'] = source_url_element['href']
    else:
        article_data['source_url'] = ''

    return article_data

def extract_ticker_symbols_from_links(soup):
    ticker_symbols = set()
    # Find all <a> tags within the article text section
    article_section = soup.select_one('div.body-wrap')
    if article_section:
        links = article_section.find_all('a', href=True)
        for link in links:
            href = link['href']
            # Check if the href contains the quote subdomain
            match = re.search(r'https://finance\.yahoo\.com/quote/([^/?]+)', href)
            if match:
                ticker_symbol = match.group(1)
                # print(ticker_symbol)
                ticker_symbols.add(ticker_symbol)
    return list(ticker_symbols)
