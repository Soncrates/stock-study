try:
    from functools import lru_cache
except ImportError:
    from backports.functools_lru_cache import lru_cache

try:
    import urlparse
    from urllib import urlencode
except: # For Python 3
    import urllib.parse as urlparse
    from urllib.parse import urlencode

import logging
import requests
import re
from bs4 import BeautifulSoup

class WEB_UTIL(object) :
  @staticmethod
  def invoke_url(url,headers=None, raw=False) :
      ret = None
      try :
          ret = WEB_UTIL._invoke_url(url,headers,raw)
      except Exception as e : print e
      return ret
  @staticmethod
  def _invoke_url(url,headers=None, raw=False) :
    if headers is not None :
      ret = requests.get(url, headers=headers)        
    else :
      ret = requests.get(url)
    if not raw : ret = ret.text
    else : ret = ret.content
    return ret
  @staticmethod
  def format_as_soup(url_response, raw=False) :
    ret = BeautifulSoup(url_response,'lxml')
    if not raw : 
      for script in ret(["script", "style"]):
            script.extract() # Remove these two elements from the BS4 object
    return ret

class YAHOO_PROFILE() :
      url = "https://finance.yahoo.com/quote/{0}/profile?p={0}"
      def __call__(self, stock) :
          parse = PROFILE_PARSE()
          url = YAHOO_PROFILE.url.format(stock)
          response = WEB_UTIL.invoke_url(url)
          soup = WEB_UTIL.format_as_soup(response)
          ret = parse(soup)
          return ret

class PROFILE_PARSE() :
      def __call__(self, soup) :
          span_list = soup.body.findAll('span')
          data = []
          for span in span_list :
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
          data = dict(zip(key_list,value_list))
          return data

if __name__ == "__main__" :
   reader = YAHOO_PROFILE()

   stock_list = ['AAPL','GOOG','SPY', 'SR-PA']
   for stock in stock_list :
       print stock
       ret = reader(stock)
       print ret

