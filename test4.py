#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版股票代码查找器
支持历史股票、退市股票的查找
"""

import json
import re
import requests
import time
from difflib import SequenceMatcher
from typing import Optional, List, Dict, Tuple
import urllib.parse


class EnhancedStockSymbolFinder:
    def __init__(self, json_file1: str = "company_tickers_exchange.json",
                 json_file2: str = "company_tickers.json"):
        """
        初始化增强版股票代码查找器

        Args:
            json_file1: 第一个JSON文件路径
            json_file2: 第二个JSON文件路径
        """
        self.companies_data = []

        # 加载本地数据
        self.load_local_data(json_file1, json_file2)

        # 常见的公司后缀
        self.company_suffixes = [
            'INC', 'CORP', 'CORPORATION', 'LTD', 'LIMITED', 'LLC', 'LP', 'LLP',
            'CO', 'COMPANY', 'HOLDINGS', 'GROUP', 'ENTERPRISES', 'SYSTEMS',
            'TECHNOLOGIES', 'TECH', 'SOLUTIONS', 'SERVICES', 'INTERNATIONAL',
            'PLC', 'SA', 'NV', 'AG', 'GMBH', 'SPA', 'BV'
        ]

    def search_online_wikipedia(self, company_name: str) -> Optional[Dict]:
        """通过Wikipedia搜索历史股票信息"""
        try:
            # 搜索Wikipedia页面
            search_url = "https://en.wikipedia.org/api/rest_v1/page/summary/"
            encoded_name = urllib.parse.quote(company_name)

            response = requests.get(f"{search_url}{encoded_name}", timeout=10)

            if response.status_code == 200:
                data = response.json()
                extract = data.get('extract', '').lower()

                # 查找股票代码的模式
                ticker_patterns = [
                    r'ticker[:\s]+([A-Z]{1,5})',
                    r'symbol[:\s]+([A-Z]{1,5})',
                    r'traded as[:\s]+([A-Z]{1,5})',
                    r'nasdaq[:\s]+([A-Z]{1,5})',
                    r'nyse[:\s]+([A-Z]{1,5})',
                ]

                for pattern in ticker_patterns:
                    match = re.search(pattern, extract, re.IGNORECASE)
                    if match:
                        ticker = match.group(1)
                        return {
                            'ticker': ticker,
                            'company_name': company_name,
                            'source': 'wikipedia',
                            'extract': data.get('extract', '')[:200] + '...'
                        }

        except Exception as e:
            print(f"Wikipedia搜索错误: {e}")

        return None

    def search_online_finviz(self, company_name: str) -> Optional[str]:
        """通过Finviz搜索股票代码（包括历史股票）"""
        try:
            # Finviz搜索
            search_url = "https://finviz.com/search.ashx"
            params = {'q': company_name}

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }

            response = requests.get(search_url, params=params, headers=headers, timeout=15)

            if response.status_code == 200:
                # 简单的HTML解析查找股票代码
                content = response.text
                # 查找ticker模式
                ticker_match = re.search(r'quote\.ashx\?t=([A-Z]{1,5})', content)
                if ticker_match:
                    return ticker_match.group(1)

        except Exception as e:
            print(f"Finviz搜索错误: {e}")

        return None

    def search_online_marketwatch(self, company_name: str) -> Optional[str]:
        """通过MarketWatch搜索股票代码"""
        try:
            # MarketWatch搜索API
            search_url = "https://www.marketwatch.com/tools/quotes/lookup.asp"
            params = {
                'siteID': 'mktw',
                'Lookup': company_name,
                'Country': 'us',
                'Type': 'All'
            }

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(search_url, params=params, headers=headers, timeout=10)

            if response.status_code == 200:
                content = response.text
                # 查找股票代码模式
                ticker_pattern = r'symbol=([A-Z]{1,5})'
                match = re.search(ticker_pattern, content)
                if match:
                    return match.group(1)

        except Exception as e:
            print(f"MarketWatch搜索错误: {e}")

        return None

    def search_delisted_stocks_online(self, company_name: str) -> Optional[Dict]:
        """专门搜索退市股票信息"""
        print(f"正在网络搜索退市股票: {company_name}")

        try:
            # 方法1: 使用Yahoo Finance历史搜索
            result = self.search_yahoo_historical(company_name)
            if result:
                return result

            # 方法2: 搜索SEC EDGAR数据
            result = self.search_sec_edgar(company_name)
            if result:
                return result

            # 方法3: 使用投资网站搜索
            result = self.search_investment_sites(company_name)
            if result:
                return result

            # 方法4: 通用网络搜索
            result = self.search_web_general(company_name)
            if result:
                return result

        except Exception as e:
            print(f"退市股票搜索错误: {e}")

        return None

    def search_yahoo_historical(self, company_name: str) -> Optional[Dict]:
        """通过Yahoo Finance搜索历史股票"""
        try:
            # Yahoo Finance有时会保留历史股票信息
            search_url = "https://query1.finance.yahoo.com/v1/finance/search"
            params = {
                'q': company_name,
                'lang': 'en-US',
                'region': 'US',
                'quotesCount': 10,
                'newsCount': 0
            }

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }

            response = requests.get(search_url, params=params, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if 'quotes' in data and data['quotes']:
                    for quote in data['quotes']:
                        # 即使是退市股票，有时也会在结果中显示
                        symbol = quote.get('symbol', '')
                        if symbol and len(symbol) <= 5:
                            print(f"Yahoo Finance历史搜索找到: {symbol}")
                            return {
                                'ticker': symbol,
                                'company_name': company_name,
                                'source': 'yahoo_historical',
                                'quote_type': quote.get('typeDisp', 'Unknown')
                            }
        except Exception as e:
            print(f"Yahoo历史搜索错误: {e}")
        return None

    def search_sec_edgar(self, company_name: str) -> Optional[Dict]:
        """搜索SEC EDGAR数据"""
        try:
            # SEC提供公司搜索API
            search_url = "https://www.sec.gov/cgi-bin/browse-edgar"
            params = {
                'company': company_name,
                'match': 'contains',
                'action': 'getcompany'
            }

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }

            response = requests.get(search_url, params=params, headers=headers, timeout=15)

            if response.status_code == 200:
                content = response.text
                # 查找Trading Symbol信息
                symbol_pattern = r'Trading Symbol[:\s]*([A-Z]{1,5})'
                match = re.search(symbol_pattern, content, re.IGNORECASE)
                if match:
                    ticker = match.group(1)
                    print(f"SEC EDGAR找到: {ticker}")
                    return {
                        'ticker': ticker,
                        'company_name': company_name,
                        'source': 'sec_edgar'
                    }

                # 替代模式：查找股票代码格式
                ticker_patterns = [
                    r'\b([A-Z]{2,5})\b.*?stock',
                    r'symbol[:\s]+([A-Z]{1,5})',
                    r'ticker[:\s]+([A-Z]{1,5})'
                ]

                for pattern in ticker_patterns:
                    matches = re.findall(pattern, content, re.IGNORECASE)
                    for match in matches:
                        if len(match) >= 2 and len(match) <= 5:
                            print(f"SEC EDGAR模式匹配找到: {match}")
                            return {
                                'ticker': match,
                                'company_name': company_name,
                                'source': 'sec_edgar_pattern'
                            }

        except Exception as e:
            print(f"SEC EDGAR搜索错误: {e}")
        return None

    def search_investment_sites(self, company_name: str) -> Optional[Dict]:
        """搜索投资网站"""
        sites = [
            {
                'name': 'MarketWatch',
                'url': 'https://www.marketwatch.com/tools/quotes/lookup.asp',
                'params': {'Lookup': company_name, 'Country': 'us'},
                'pattern': r'symbol=([A-Z]{1,5})'
            },
            {
                'name': 'Investing.com',
                'url': 'https://www.investing.com/search/',
                'params': {'q': company_name},
                'pattern': r'/equities/([a-z-]+)'  # 会找到URL模式，需要进一步处理
            }
        ]

        for site in sites:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }

                response = requests.get(site['url'], params=site['params'],
                                        headers=headers, timeout=10)

                if response.status_code == 200:
                    content = response.text
                    matches = re.findall(site['pattern'], content, re.IGNORECASE)

                    for match in matches:
                        if site['name'] == 'MarketWatch' and len(match) <= 5:
                            print(f"{site['name']}找到: {match}")
                            return {
                                'ticker': match,
                                'company_name': company_name,
                                'source': site['name'].lower()
                            }

                time.sleep(1)  # 避免请求过快

            except Exception as e:
                print(f"{site['name']}搜索错误: {e}")
                continue

        return None

    def search_web_general(self, company_name: str) -> Optional[Dict]:
        """通用网络搜索"""
        try:
            # 构造更精确的搜索查询
            search_queries = [
                f'"{company_name}" stock ticker symbol',
                f'"{company_name}" NYSE NASDAQ symbol',
                f'{company_name} delisted merger acquisition ticker',
                f'{company_name} stock symbol',
                f'{company_name} ticker'
            ]

            for query in search_queries:
                print(f"网络搜索: {query}")

                # 使用DuckDuckGo搜索
                search_url = "https://html.duckduckgo.com/html/"
                params = {'q': query}

                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }

                try:
                    response = requests.get(search_url, params=params, headers=headers, timeout=15)
                    if response.status_code == 200:
                        content = response.text

                        # 更严格的模式匹配股票代码
                        patterns = [
                            # 标准格式：公司名 + 股票代码
                            rf'{re.escape(company_name.lower())}[^a-zA-Z]*ticker[:\s]*([A-Z]{{2,5}})',
                            rf'{re.escape(company_name.lower())}[^a-zA-Z]*symbol[:\s]*([A-Z]{{2,5}})',
                            rf'ticker[:\s]*([A-Z]{{2,5}})[^a-zA-Z]*{re.escape(company_name.lower())}',
                            rf'symbol[:\s]*([A-Z]{{2,5}})[^a-zA-Z]*{re.escape(company_name.lower())}',

                            # 括号格式
                            rf'{re.escape(company_name.lower())}[^a-zA-Z]*\(([A-Z]{{2,5}})\)',
                            rf'\(([A-Z]{{2,5}})\)[^a-zA-Z]*{re.escape(company_name.lower())}',

                            # NYSE/NASDAQ格式
                            rf'{re.escape(company_name.lower())}[^a-zA-Z]*nyse[:\s]*([A-Z]{{2,5}})',
                            rf'{re.escape(company_name.lower())}[^a-zA-Z]*nasdaq[:\s]*([A-Z]{{2,5}})',
                        ]

                        # 验证找到的股票代码
                        for pattern in patterns:
                            matches = re.findall(pattern, content.lower(), re.IGNORECASE | re.DOTALL)
                            if matches:
                                for match in matches:
                                    candidate = match.upper()
                                    # 验证股票代码的有效性
                                    if self.is_valid_ticker(candidate):
                                        print(f"网络搜索找到: {candidate}")
                                        return {
                                            'ticker': candidate,
                                            'company_name': company_name,
                                            'source': 'web_search',
                                            'query': query,
                                            'pattern': pattern
                                        }

                    time.sleep(2)  # 避免被封IP

                except Exception as e:
                    print(f"搜索查询错误: {e}")
                    continue

        except Exception as e:
            print(f"通用网络搜索错误: {e}")

        return None

    def is_valid_ticker(self, ticker: str) -> bool:
        """验证股票代码是否有效"""
        if not ticker or not isinstance(ticker, str):
            return False

        # 基本格式检查
        if not (2 <= len(ticker) <= 5):
            return False

        if not ticker.isupper() or not ticker.isalpha():
            return False

        # 排除常见的非股票代码词汇
        invalid_words = {
            'HTML', 'HTTP', 'HTTPS', 'WWW', 'COM', 'ORG', 'NET', 'GOV',
            'THE', 'AND', 'FOR', 'WITH', 'FROM', 'THIS', 'THAT',
            'MORE', 'ABOUT', 'CONTACT', 'NEWS', 'INFO', 'HELP',
            'PAGE', 'SITE', 'LINK', 'HREF', 'TEXT', 'FONT',
            'COLOR', 'SIZE', 'STYLE', 'CLASS', 'SPAN', 'DIV'
        }

        if ticker in invalid_words:
            return False

        # 检查是否看起来像HTML标签或属性
        if ticker.startswith(('HT', 'HR', 'SR', 'CL', 'ST', 'SP', 'DI')):
            return False

        return True

    def load_local_data(self, json_file1: str, json_file2: str):
        """加载本地JSON文件数据"""
        try:
            with open(json_file1, 'r', encoding='utf-8') as f:
                data1 = json.load(f)
                if 'data' in data1 and 'fields' in data1:
                    fields = data1['fields']
                    name_idx = fields.index('name')
                    ticker_idx = fields.index('ticker')
                    for row in data1['data']:
                        self.companies_data.append({
                            'name': row[name_idx],
                            'ticker': row[ticker_idx],
                            'source': 'file1'
                        })
        except FileNotFoundError:
            print(f"警告: 无法找到文件 {json_file1}")
        except Exception as e:
            print(f"加载文件 {json_file1} 时出错: {e}")

        try:
            with open(json_file2, 'r', encoding='utf-8') as f:
                data2 = json.load(f)
                for key, company in data2.items():
                    if isinstance(company, dict) and 'title' in company and 'ticker' in company:
                        existing_tickers = [comp['ticker'] for comp in self.companies_data]
                        if company['ticker'] not in existing_tickers:
                            self.companies_data.append({
                                'name': company['title'],
                                'ticker': company['ticker'],
                                'source': 'file2'
                            })
        except FileNotFoundError:
            print(f"警告: 无法找到文件 {json_file2}")
        except Exception as e:
            print(f"加载文件 {json_file2} 时出错: {e}")

        print(f"成功加载 {len(self.companies_data)} 家公司的数据")

    def normalize_company_name(self, name: str) -> str:
        """标准化公司名称"""
        name = name.upper().strip()
        name = re.sub(r'[.,&\-/]', ' ', name)
        name = re.sub(r'\s+', ' ', name)

        words = name.split()
        filtered_words = []
        for word in words:
            if word not in self.company_suffixes:
                filtered_words.append(word)

        return ' '.join(filtered_words).strip()

    def calculate_similarity(self, name1: str, name2: str) -> float:
        """计算两个公司名称的相似度"""
        norm1 = self.normalize_company_name(name1)
        norm2 = self.normalize_company_name(name2)

        # 完全匹配
        if norm1 == norm2:
            return 1.0

        # 避免错误的部分匹配（如 ALLERGAN 和 ARGAN）
        # 检查是否一个是另一个的真正子串
        if norm1 and norm2:
            # 如果两个名称长度差异很大，降低相似度
            len_diff = abs(len(norm1) - len(norm2))
            if len_diff > max(len(norm1), len(norm2)) * 0.3:  # 长度差异超过30%
                base_ratio = SequenceMatcher(None, norm1, norm2).ratio()
                # 对长度差异很大的情况进行惩罚
                return base_ratio * (1 - len_diff / max(len(norm1), len(norm2)))

            # 检查是否是真正的包含关系
            if norm1 in norm2 or norm2 in norm1:
                shorter = norm1 if len(norm1) < len(norm2) else norm2
                longer = norm2 if len(norm1) < len(norm2) else norm1

                # 如果较短的名称占较长名称的比例很高，才认为是包含关系
                if len(shorter) >= len(longer) * 0.7:
                    return 0.9
                else:
                    # 否则降低相似度
                    return SequenceMatcher(None, norm1, norm2).ratio() * 0.8

        # 序列匹配
        return SequenceMatcher(None, norm1, norm2).ratio()

    def search_local(self, company_name: str, threshold: float = 0.8) -> List[Tuple[Dict, float]]:
        """在本地数据中搜索"""
        results = []

        for company in self.companies_data:
            similarity = self.calculate_similarity(company_name, company['name'])
            if similarity >= threshold:
                results.append((company, similarity))

        results.sort(key=lambda x: x[1], reverse=True)

        # 额外检查：如果最佳匹配的相似度不够高，可能是错误匹配
        if results and results[0][1] < 0.85:
            print(f"警告：最佳本地匹配相似度较低 ({results[0][1]:.2f})，可能不准确")
            # 可以选择不返回低相似度的结果，强制进行在线搜索
            if results[0][1] < 0.82:
                print("相似度过低，跳过本地匹配，直接进行在线搜索")
                return []

        return results

    def find_symbol(self, company_name: str, use_online: bool = True) -> Optional[Dict]:
        """
        查找公司的股票代码（包括历史股票）

        Args:
            company_name: 公司名称
            use_online: 是否使用在线搜索

        Returns:
            包含股票代码和相关信息的字典
        """
        print(f"\n正在搜索: {company_name}")

        # 1. 首先在本地数据中搜索
        local_results = self.search_local(company_name)
        if local_results:
            best_match = local_results[0]
            company_info = best_match[0]
            similarity = best_match[1]

            print(f"本地匹配找到: {company_info['name']} -> {company_info['ticker']}")
            return {
                'ticker': company_info['ticker'],
                'company_name': company_info['name'],
                'similarity': similarity,
                'source': 'local',
                'status': 'active'
            }

        # 2. 在线搜索
        if use_online:
            print("尝试在线搜索退市股票信息...")
            delisted_result = self.search_delisted_stocks_online(company_name)
            if delisted_result:
                ticker = delisted_result['ticker']
                print(f"找到退市股票: {company_name} -> {ticker}")

                return {
                    'ticker': ticker,
                    'company_name': company_name,
                    'source': 'web_search',
                    'status': 'delisted',
                    'search_method': delisted_result.get('query')
                }

            # Wikipedia搜索
            wiki_result = self.search_online_wikipedia(company_name)
            if wiki_result:
                ticker = wiki_result['ticker']
                print(f"Wikipedia找到: {company_name} -> {ticker}")
                return wiki_result

            # 其他在线搜索方法...
            print("标准在线搜索未找到结果")

        print("未找到匹配的股票代码")
        return None


def main():
    """主函数 - 交互式界面"""
    print("=" * 70)
    print("股票代码查找器 (支持历史股票)")
    print("=" * 70)
    print("功能特点：")
    print("- 支持当前上市公司股票代码查找")
    print("- 支持历史/退市股票代码查找")
    print("- 多数据源在线搜索")
    print("=" * 70)

    # 初始化查找器
    finder = EnhancedStockSymbolFinder()

    while True:
        try:
            company_name = input("\n请输入公司名称（输入quit以退出）: ").strip()

            if not company_name:
                continue

            if company_name.lower() in ['quit', 'exit', '退出', 'q']:
                break

            # 查找股票代码
            result = finder.find_symbol(company_name, use_online=True)

            if result:
                print("\n" + "=" * 50)
                print(f"公司名称: {result['company_name']}")
                print(f"股票代码: {result['ticker']}")
                print(f"数据源: {result['source']}")
                print(f"状态: {result.get('status', 'unknown')}")

                if result.get('similarity'):
                    print(f"匹配度: {result['similarity']:.2f}")

                if result.get('search_method'):
                    print(f"搜索方法: {result['search_method']}")

                print("=" * 50)
            else:
                print(f"\n抱歉，未找到 '{company_name}' 的股票代码")

        except Exception as e:
            print(f"发生错误: {e}")


if __name__ == "__main__":
    main()