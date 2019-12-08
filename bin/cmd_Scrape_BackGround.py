#!/usr/bin/env python

import re
import logging
from multiprocessing import Pool
from libCommon import INI, log_exception
from libDebug import trace
from libNASDAQ import NASDAQ
from libWeb import WEB_UTIL

'''
   Web Scraper
   Use RESTful interface to to get web pages and parse for relevant info about stocks and funds
'''

class DICT_HELPER() :
    def __init__(self, *largs, **kwargs):
        self.data = kwargs
    def __delitem__(self, key):
        value = self.data.pop(key)
        self.data.pop(value, None)
    def __setitem__(self, key, value):
        if key in self.data:
            del self.data[key]
        self.data[key] = value
    def __getitem__(self, key):
        return self.data[key]
    def __str__(self):
        msg = map(lambda x : "{} : {}".format(x,len(self.data[x])), sorted(self.data))
        msg.append("Total : {}".format(len(self.values())))
        return "\n".join(msg)
    def append(self, key, *value):
        if key not in self.data:
           self.data[key] = []
        self.data[key].append(value)
    def values(self):
        ret = reduce(lambda a, b : a+b, self.data.values())
        ret = sorted(list(set(ret)))
        return ret
    @classmethod
    def init(cls, *largs, **kwargs) :
        ret = cls(*largs, **kwargs)
        return ret

class YAHOO() :
      url = "https://finance.yahoo.com/quote/{0}/profile?p={0}"
      @classmethod
      def get(cls, stock) :
          url = cls.url.format(stock)
          response = WEB_UTIL.invoke_url(url)
          soup = WEB_UTIL.format_as_soup(response)
          ret = cls.parse(soup)
          ret['Stock'] = stock
          logging.debug(ret)
          return ret
      @classmethod
      def parse(cls, soup) :
          if soup is None : return {}
          if soup.body is None : return {}
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
          ret = dict(zip(key_list,value_list))
          logging.debug(ret)
          return ret
      @classmethod
      @trace
      def scrape(cls, stock_list) :
          ret = DICT_HELPER.init()
          _sector = 'Sector'
          for row in stock_list :
              stock = cls.get(row)
              sector = stock.get(_sector,None) 
              if not sector :
                 continue
              ret.append(sector,row)
          logging.info(ret)
          _stock_list = ret.values()
          return ret, _stock_list

class FINANCEMODELLING_STOCK_LIST() :
      url = "https://financialmodelingprep.com/api/v3/company/stock/list"
      @classmethod
      def get(cls) :
          response = WEB_UTIL.json(cls.url)
          target = "symbolsList"
          ret = response.get(target,{})
          logging.debug(ret)
          return ret

class FINANCEMODELLING() :
      url = "https://financialmodelingprep.com/api/v3/company/profile/{0}"
      @classmethod
      def get(cls, stock) :
          url = cls.url.format(stock)
          response = WEB_UTIL.json(url)
          target = "profile"
          ret = response.get(target,{})
          ret['Stock'] = stock
          logging.debug(ret)
          return ret
      @classmethod
      @trace
      def scrape(cls, stock_list) :
          _stock_list = map(lambda x : x.get('symbol', None), FINANCEMODELLING_STOCK_LIST.get())
          stock_list = set(_stock_list).intersection(set(stock_list))
          stock_list = sorted(list(stock_list))

          ret = DICT_HELPER.init()
          _sector = 'sector'
          for row in stock_list:
              try :
                 stock = cls.get(row)
              except :
                 stock = None
              if not stock : continue
              sector = stock.get(_sector,None) 
              if not sector :
                 continue
              ret.append(sector,row)
          logging.info(ret)
          _stock_list = ret.values()
          return ret, _stock_list

class STOCKMONITOR() :
      url = 'https://www.stockmonitor.com/sector/{}'
      url_list = [
      'basic-materials'
      , 'communication-services'
      , 'consumer-cyclical'
      , 'consumer-defensive'
      , 'energy'
      , 'financial-services'
      , 'healthcare'
      , 'industrials'
      , 'technology'
      , 'utilities'
      ]
      def __init__(self, url_list) :
          self.url_list = url_list
      def __str__(self) :
          msg = self.url_list.values()
          msg = '\n'.join(sorted(msg))
          return msg
      def scrape(self) :
          ret = DICT_HELPER.init()
          for sector in sorted(self.url_list) : 
              logging.debug(sector)
              url = self.url_list[sector]
              stock_list = STOCKMONITOR.get(url)
              ret[sector] = stock_list
          logging.info(ret)
          _stock_list = ret.values()
          return ret, _stock_list 

      @classmethod
      def init(cls) :
          key_list = map(lambda x : x.replace('-','_'), cls.url_list)
          url_list = map(lambda x : cls.url.format(x), cls.url_list)
          url_list = dict(zip(key_list,url_list))
          ret = cls(url_list)
          logging.info(ret)
          return ret
      @classmethod
      def get(cls,url) :
          response = WEB_UTIL.invoke_url(url)
          soup = WEB_UTIL.format_as_soup(response)
          ret = set([])
          for link in soup.findAll('a', attrs={'href': re.compile("^/quote")}):
              ret.add( link.contents[0] )
          return sorted(list(ret))

def handle_alias(*stock_list,**alias) :
    ret = set(alias.keys()).intersection(set(stock_list))
    left_overs = set(stock_list) - ret
    ret = sorted(list(ret))
    left_overs = sorted(list(left_overs))
    logging.info(ret)
    retry = map(lambda x : alias[x].values(), ret)
    retry = reduce(lambda a, b : a+b, retry)
    return ret, retry, left_overs 

@log_exception
@trace
def main(save_file) :
    stock_list, etf_list, alias = NASDAQ.init().stock_list()

    sm, stocks = STOCKMONITOR.init().scrape()
    stock_list = set(stock_list) - set(stocks)
    stock_list = sorted(list(stock_list))

    fm, stocks = FINANCEMODELLING.scrape(stock_list)
    stock_list = set(stock_list) - set(stocks)
    stock_list = sorted(list(stock_list))

    y, stocks = YAHOO.scrape(stock_list)
    stock_list = set(stock_list) - set(stocks)
    stock_list = sorted(list(stock_list))

    '''
    Try a set of alternative names
    '''
    _retry, retry, stock_list = handle_alias(*stock_list,**alias) 

    fm2, stocks = scrape_financemodelling(retry)
    retry = set(retry) - set(stocks)
    retry = sorted(list(retry))

    y2, stocks = scrape_yahoo(retry)
    retry = set(retry) - set(stocks)
    retry = sorted(list(retry))

    logging.info(retry)
    logging.info(temp_fm)
    logging.info(temp_y)
    logging.info(_retry)

    config = INI.init()
    INI.write_section(config,"STOCKMONITOR",**sm)
    INI.write_section(config,"FINANCEMODEL",**fm)
    INI.write_section(config,"YAHOO",**y)
    INI.write_section(config,"FINANCEMODEL2",**fm2)
    INI.write_section(config,"YAHOO2",**y2)
    INI.write_section(config,"NASDAQTRADER",**{'unkown' : stock_list , 'alias' : retry })
    for name in sorted(alias) :
        INI.write_section(config,name,**alias[name])
    config.write(open(save_file, 'w'))

if __name__ == '__main__' :

   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   stock_ini = '{}/local/stock_background.ini'.format(env.pwd_parent)
   main(stock_ini)
