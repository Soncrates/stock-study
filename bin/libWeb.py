try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache

'''
try:
    import urlparse
    from urllib import urlencode
except: # For Python 3
    import urllib.parse as urlparse
    from urllib.parse import urlencode

'''
import logging
import re

from libUtils import WEB as WEB_UTIL
'''

Web Scraping Utils


https://financialmodelingprep.com/developer/docs/
'''

'''
   Downloading background profiles via yahoo api takes 90 minutes for ~8K calls
   Implementing async calls reduces the time to 8 minutes, but after the first 100 results, 
     the yahoo servers detect scraping and begin returning 404 for all subsequents calls.
     The Bastards.

'''
class CONSTANTS() :
      YAHOO_BASE_URL = "https://finance.yahoo.com"
      YAHOO_PROFILE = YAHOO_BASE_URL + "/quote/{0}/profile?p={0}"

      FINANCE_BASE_URL = "https://financialmodelingprep.com"
      FINANCE_STOCK_LIST = FINANCE_BASE_URL + "/api/v3/company/stock/list"
      STOCK_LIST_FIELD = "symbolsList"
      FINANCE_INDEX = FINANCE_BASE_URL + "/api/v3/majors-indexes"
      INDEX_FIELD = "majorIndexesList"
      FINANCE_PROFILE = FINANCE_BASE_URL + "/api/v3/company/profile/{0}"
      PROFILE_FIELD= "profile"

class YAHOO_PROFILE() :

      @classmethod
      def get(cls, stock) :
          url = CONSTANTS.YAHOO_PROFILE.format(stock)
          response = WEB_UTIL.invoke_url(url)
          soup = WEB_UTIL.format_as_soup(response)
          ret = YAHOO_PROFILE_PARSE.parse(soup)
          ret['Stock'] = stock
          logging.info(ret)
          return ret
'''
Stocks and Funds on the nasdaq have the same api but very different content
'''
class YAHOO_PROFILE_PARSE() :
      def __call__(self, soup) :
          return YAHOO_PROFILE_PARSE.parse(soup)
      @classmethod
      def parse(cls, soup) :
          if soup is None : return {}
          if soup.body is None : return {}
          span_list = soup.body.findAll('span')
          data = []
          for i, span in enumerate(span_list) :
              data.append(span.text)
          if len(data) == 0 :
             return {}
          while True :
                if data[0] == 'Sector' :
                   break
                if data[0] == 'Category' :
                   break
                data = data[1:]
                if len(data) == 0 :
                   return {}
          logging.debug(data)
          key_list = data[0:10:2]
          value_list = data[1:10:2]
          ret = dict(zip(key_list,value_list))
          logging.debug(ret)
          return ret

class FINANCEMODELLING_STOCK_LIST() :

      @classmethod
      def get(cls) :
          response = WEB_UTIL.json(CONSTANTS.FINANCE_STOCK_LIST)
          ret = response.get(CONSTANTS.STOCK_LIST_FIELD,{})
          return ret

class FINANCEMODELLING_INDEX() :

      @classmethod
      def get(cls) :
          response = WEB_UTIL.json(CONSTANTS.FINANCE_INDEX)
          ret = response.get(CONSTANTS.INDEX_FIELD,{})
          return ret

class FINANCEMODELLING_PROFILE() :

      @classmethod
      def get(cls, stock) :
          url = CONSTANTS.FINANCE_PROFILE.format(stock)
          response = WEB_UTIL.json(url)
          ret = response.get(CONSTANTS.PROFILE_FIELD,{})
          ret['Stock'] = stock
          return ret

if __name__ == "__main__" :
   from multiprocessing import Pool
   def sync(stock):
       return YAHOO_PROFILE.get(stock)

   def worker(pool_size, *stock_list):
       pool = Pool(pool_size)
       logging.debug(stock_list)
       ret = pool.map(sync,stock_list)
       return ret

   reader = YAHOO_PROFILE()
   reader = FINANCEMODELLING_PROFILE()

   ret_list = FINANCEMODELLING_STOCK_LIST.get()
   for i,ret in enumerate(ret_list) :
       print ((i, ret))
   stock_list = ['AAPL','GOOG','SPY', 'SRCpA','SRC-A', 'SRC$A', 'SRCA']
   print (worker(5,*stock_list))
   for i, stock in enumerate(stock_list) :
       print ((i,stock))
       ret = reader(stock)
       print ((i,ret))
   for index in FINANCEMODELLING_INDEX.get() :
       print (index)

