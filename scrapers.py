class NasdaqScraper :
    def __init__(self,exchange_list=None,unwanted_keys_list=None) : 
        self.exchanges=["nyse", "nasdaq"]
        self.unwanted_keys=['Summary Quote','MarketCap','LastSale','IPOyear']
        if exchange_list is not None  : self.exchanges = exchange_list
        if unwanted_keys_list is not None  : self.unwanted_keys = unwanted_keys_list
    def __call__(self) :
      ret = {}
      for url, headers in get_ticker_urls(self.exchanges) :
        csv = invoke_url(url,headers)
        for stock in parse_csv_stock_symbol_symbols(csv,self.unwanted_keys) :
          symbol=stock['Symbol']
          del stock['Symbol']
          ret[symbol] = stock
      return ret
class YahooScraper :
    def __init__(self,data_get,data_parse) :
        self.get = data_get
        self.parse = data_parse
    def __call__(self,symbol) :
        ret = self.get(symbol)
        for token in self.parse(ret) :
            yield token
yahoo_cash_flow = YahooScraper(get_yahoo_cash_flow_soup,parse_yahoo)
yahoo_income_statement = YahooScraper(get_yahoo_income_statement_soup,parse_yahoo)
yahoo_balance_sheet = YahooScraper(get_yahoo_balance_sheet_soup,parse_yahoo)
yahoo_analyst_estimates_soup = YahooScraper(get_yahoo_analyst_estimates_soup,parse_yahoo)
