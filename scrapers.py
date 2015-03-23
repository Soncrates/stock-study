import scraper-util.py
import time-util.py

class BaseScraper :
  def __init__(self,data_get,data_parse, data_formatter=None) :
    self.get_data = data_get
    self.parse_data = data_parse
    self.data_formatter = data_formatter

class YahooScraper(BaseScraper) :
    def __init__(self,data_get,data_format,data_parse) :
      BaseScraper.__init__( self,data_get,data_parse,data_format)
    def __call__(self,symbol) :
      ret = self.get_data(symbol)
      print (ret)
      if self.data_formatter is not None :
        ret = self.data_formatter(ret)
      for token in self.parse_data(ret) :
        yield token

class NasdaqScraper(BaseScraper) :
  def __init__(self,data_get,data_parse,data_formatter,exchange_list=None,unwanted_keys_list=None) : 
    BaseScraper.__init__( self,data_get,data_parse,data_formatter)
    self.exchanges=["nyse", "nasdaq"]
    self.unwanted_keys=['Summary Quote','MarketCap','LastSale','IPOyear','Unnamed: 8']
    if exchange_list is not None  : self.exchanges = exchange_list
    if unwanted_keys_list is not None  : self.unwanted_keys = unwanted_keys_list
  def __call__(self,exchange_list=None,unwanted_keys_list=None) :
    exchanges = self.exchanges
    unwanted_keys = self.unwanted_keys
    if exchange_list is not None  : exchanges = exchange_list
    if unwanted_keys_list is not None  : unwanted_keys = unwanted_keys_list
    ret = None
    for exchange in exchanges : 
      if ret is None : ret = self.get_data(exchange)
      else : ret = b"".join([ret, self.get_data(exchange)])
    ret = self.parse_data(ret)
    if self.data_formatter is not None :
      ret = self.data_formatter(ret,unwanted_keys,exchange)
    return ret.reindex()
class NasdaqService() :
    def __init__(self,service) :
        self.service = service
        self.fresh = None
        self.cache = None
    def __call__(self) :
        if self.cache is None or not self.fresh(): 
            self.cache = self.service()
            self.fresh = ExpireTimer(24*60) 
        return self.cache
class StockService() :
    def __init__(self) :
        self.fresh = {}
        self.cache = {}
    def __call__(self,stock) :
        if stock not in self.cache.keys() or not self.fresh[stock](): 
            y1,y2,r = get_year_paramters()
            self.cache[stock] = get_yahoo_historical(stock,y1)
            self.fresh[stock] = ExpireTimer(24*60) 
        return self.cache[stock]

class YQLQuery(object):
  _YAHOO_URL = 'http://query.yahooapis.com/v1/public/yql'
  _DATATABLES_URL = 'store://datatables.org/alltableswithkeys'
  _quotes = 'yahoo.finance.quotes'
  _options = 'yahoo.finance.options'
  _quoteslist = 'yahoo.finance.quoteslist'
  _sectors = 'yahoo.finance.sectors'
  _industry = 'yahoo.finance.industry'
  def __init__(self):
  def __call__(self, yql):
    import urllib
    import simplejson as json
    queryString = urllib.urlencode({'q': yql, 'format': 'json', 'env': self._DATATABLES_URL})
    url = self._YAHOO_URL  + '?' + queryString
    ret = invoke_url(url)
    ret = json.loads(ret)
    return ret
  def __is_valid_response(self, response, field):
    return 'query' in response and 'results' in response['query'] and field in response['query']['results']
  def __validate_response(self, response, tagToCheck):
    if self.__is_valid_response(response, tagToCheck):
      ret = response['query']['results'][tagToCheck]
    else:
      if 'error' in response:
        raise QueryError('YQL query failed with error: "%s".' % response['error']['description'])
      else:
        raise QueryError('YQL response malformed.')
    return ret
class YQLStock(object) :
  def __init__(self,query,table,tag) :
    self.query = query
    self.table = table
	  self.tag = tag
  def __call__(self,symbol, columns='*') :
    yql = 'select %s from %s where symbol in (%s)' %(columns, self.table, symbol)
    ret = self.query(yql)
    return self.query.__validate_response(ret, self.tag)
class YQLStockQuote(YQLStock) :
  def __init__(self) :
    YQLStock.__init__(YQLQuery(),YQLQuery._quotes,'quote')
class YQLStockOptions(YQLStock) :
  def __init__(self) :
    YQLStock.__init__(YQLQuery(),YQLQuery._options,'optionsChain')
class YQLStockQuoteSummary(YQLStock) :
  def __init__(self) :
    YQLStock.__init__(YQLQuery(),YQLQuery._quoteslist,'quote')
class YQLStockNameBySector(YQLStock) :
  def __init__(self) :
    YQLStock.__init__(YQLQuery(),YQLQuery._sectors,'sector')
  def __call__(self)
    yql = 'select * from %s' % self.table
    ret = self.query(yql)
    return self.query.__validate_response(ret, self.tag)
class YQLStockByIndustry(YQLStock) :
  def __init__(self) :
    YQLStock.__init__(YQLQuery(),YQLQuery._industry,'industry')
  def __call__(self,id)
    yql = 'select * from %s where id =\'%s\'' % self.table, id)
    ret = self.query(yql)
    return self.query.__validate_response(ret, self.tag)
class YQLNews(object) :
  RSS_URL = 'http://finance.yahoo.com/rss/headline?s='
  def __init__(self,query,table,tag) :
    self.query = query
  def __call__(self,symbol) :
    yql = 'select title, link, description, pubDate from rss where url=\'%s%s\'' % RSS_URL,symbol
    ret = self.query(yql)['query']['results']['item']
    if ret[0]['title'].find('not found') > 0:
      raise QueryError('Feed for %s does not exist.' % symbol)
    else:
      return ret
      
yahoo_cash_flow = YahooScraper(get_yahoo_cash_flow,format_as_soup,parse_yahoo)
yahoo_income_statement = YahooScraper(get_yahoo_income_statement,format_as_soup,parse_yahoo)
yahoo_balance_sheet = YahooScraper(get_yahoo_balance_sheet,format_as_soup,parse_yahoo)
yahoo_analyst_estimates_soup = YahooScraper(get_yahoo_analyst_estimates,format_as_soup,parse_yahoo)

nasdaqscraper = NasdaqService(get_nasdaq_csv,parse_csv,format_nasdaq)
nasdaq = NasdaqService(nasdaqscraper)
