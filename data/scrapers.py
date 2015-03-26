import scraper-util.py
import soup-util.py
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
            y1,y2,r = get_year_parameters()
            self.cache[stock] = get_yahoo_historical(stock,y1)
            self.fresh[stock] = ExpireTimer(24*60) 
        return self.cache[stock]
class YQLNewsItem(object) :
    RSS_URL = 'http://finance.yahoo.com/rss/headline?s='
    def __init__(self,query=YQLQuery()) :
        self.query = query
    def __call__(self,symbol) :
        if '^' in symbol : return None
        yql = 'select title,link, pubDate from rss where url=\'%s%s\'' % (YQLNews.RSS_URL,symbol)
        ret = self.query(yql)
        if ret is None  :return None
        if 'query' in ret : ret = ret['query']
        if ret is None  :return None
        if 'results' in ret : ret = ret['results']
        if ret is None  :return None
        if 'item' in ret : ret = ret['item']
        if ret is None  :return None
        print (type(ret))
        if isinstance(ret,list) :
            if 'title' in ret[0] and ret[0]['title'].find('not found') > 0:
                raise IOError('Feed for %s does not exist.' % symbol)
        elif isinstance(ret,dict) :
            if 'error' in ret :
                raise IOError('Failed to read feed for %s : %s ' % (symbol,ret['error'])) 
            else :
                print(ret.keys())
        else :
            print(type(ret))
        return ret
class YQLNewsList(object) :
    def __init__(self,query=YQLNewsItem()) :
        self.query = query
        self.fresh = {}
        self.cache = {}
    def __call__(self,stock) :
        if stock not in self.cache.keys() or not self.fresh[stock](): 
            ret = self.query(stock)
            self.fresh[stock] = ExpireTimer(24*60)
            self.cache[stock] = self.__transform(ret)
        return self.cache[stock]
    def __transform(self,rss) :
        if rss is None : return rss
        for item in rss :
            del (item['title'])
            url = item['link'].split('*')[1]
            soup = format_noodle(url)
            if soup is None : soup = format_yahoo_finance(url)
            if soup is None : soup = format_biz_yahoo(url)
            if soup is None : soup = format_investopedia(url)
            if soup is None : soup = format_generic(url)
            item['link'] = soup
        return rss
