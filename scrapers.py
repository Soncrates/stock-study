import scraper-util.py

class BaseScraper :
    def __init__(self,data_get,data_parse) :
        self.get_data = data_get
        self.parse_data = data_parse

class YahooScraper(BaseScraper) :
    def __init__(self,data_get,data_parse) :
      BaseScraper.__init__( self,data_get,data_parse)
    def __call__(self,symbol) :
      import BeautifulSoup
      ret = self.get_data(symbol)
      for token in self.parse_data(ret) :
        yield token

class NasdaqScraper(BaseScraper) :
    def __init__(self,data_get,data_parse,exchange_list=None,unwanted_keys_list=None) : 
        BaseScraper.__init__( self,data_get,data_parse)
        self.exchanges=["nyse", "nasdaq"]
        self.unwanted_keys=['Summary Quote','MarketCap','LastSale','IPOyear']
        if exchange_list is not None  : self.exchanges = exchange_list
        if unwanted_keys_list is not None  : self.unwanted_keys = unwanted_keys_list
    def __call__(self,exchange_list=None,unwanted_keys_list=None) :
      exchanges = self.exchanges
      unwanted_keys = self.unwanted_keys
      if exchange_list is not None  : exchanges = exchange_list
      if unwanted_keys_list is not None  : unwanted_keys = unwanted_keys_list
      ret = {}
      for exchange in exchanges :
        data = self.get_data(exchange)
        for ele in self.parse_data(data,unwanted_keys) :
          symbol=ele['Symbol']
          del ele['Symbol']
          ele['Exchange'] = exchange
          ret[symbol] = ele
      return ret
        
nasdaq = NasdaqScraper(get_csv_from_nasdaq,parse_csv_from_nasdaq)
yahoo_cash_flow = YahooScraper(get_yahoo_cash_flow_soup,parse_yahoo)
yahoo_income_statement = YahooScraper(get_yahoo_income_statement_soup,parse_yahoo)
yahoo_balance_sheet = YahooScraper(get_yahoo_balance_sheet_soup,parse_yahoo)
yahoo_analyst_estimates_soup = YahooScraper(get_yahoo_analyst_estimates_soup,parse_yahoo)
