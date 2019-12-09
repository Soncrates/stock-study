#!/usr/bin/env python

import re
import logging
from multiprocessing import Pool
from libCommon import INI, log_exception
from libDebug import trace
from libNASDAQ import NASDAQ
from libWeb import WEB_UTIL, YAHOO_PROFILE
from libWeb import FINANCEMODELLING_STOCK_LIST as STOCK_LIST, FINANCEMODELLING_PROFILE


'''
   Web Scraper
   Use RESTful interface to to get web pages and parse for relevant info about stocks and funds
'''
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
      def __call__(self) :
          ret = {}
          total = 0
          for sector in sorted(self.url_list) : 
              logging.debug(sector)
              url = self.url_list[sector]
              stock_list = STOCKMONITOR.get(url)
              logging.info((sector,len(stock_list)))
              ret[sector] = stock_list
              total += len(stock_list)
          logging.info(("Total",total))
          return ret

      @classmethod
      def init(cls) :
          key_list = map(lambda x : x.replace('-','_'), cls.url_list)
          url_list = map(lambda x : cls.url.format(x), cls.url_list)
          url_list = dict(zip(key_list,url_list))
          ret = cls(url_list)
          return ret
      @classmethod
      def get(cls,url) :
          response = WEB_UTIL.invoke_url(url)
          soup = WEB_UTIL.format_as_soup(response)
          ret = set([])
          for link in soup.findAll('a', attrs={'href': re.compile("^/quote")}):
              ret.add( link.contents[0] )
          return sorted(list(ret))
def scrape_yahoo(stock_list) :
    ret = {}
    _sector = 'Sector'
    for row in stock_list :
        stock = YAHOO_PROFILE.get(row)
        sector = stock.get(_sector,None) 
        if not sector :
           continue
        if sector not in ret :
           ret[sector] = []
        ret[sector].append(row)
    return ret

def scrape_financemodelling(stock_list) :
    _stock_list = map(lambda x : x.get('symbol', None), STOCK_LIST.get())
    stock_list = set(_stock_list).intersection(set(stock_list))
    stock_list = sorted(list(stock_list))

    ret = {}
    _sector = 'sector'
    for row in stock_list:
        try :
           stock = FINANCEMODELLING_PROFILE.get(row)
        except :
           stock = None
        if not stock : continue
        sector = stock.get(_sector,None) 
        if not sector :
           continue
        if sector not in ret :
           ret[sector] = []
        ret[sector].append(row)
    return ret

def handle_alias(*stock_list,**alias) :
    ret = set(alias.keys()).intersection(set(stock_list))
    left_overs = set(stock_list) - ret
    ret = sorted(list(ret))
    left_overs = sorted(list(left_overs))
    logging.info(ret)
    return ret, left_overs 

@log_exception
@trace
def main(save_file) :
    stock, etf, alias = NASDAQ.init().stock_list()

    background = STOCKMONITOR.init()
    logging.info(background)
    stockmonitor = background()
    temp = reduce(lambda a, b : a+b, stockmonitor.values())
    stock = set(stock) - set(temp)
    stock_list = sorted(list(stock))

    FinanceModel = scrape_financemodelling(stock_list)
    temp = reduce(lambda a, b : a+b, FinanceModel.values())
    stock = set(stock) - set(temp)
    stock_list = sorted(list(stock))

    yahoo = scrape_yahoo(stock_list)
    temp = reduce(lambda a, b : a+b, yahoo.values())
    stock = set(stock) - set(temp)
    stock_list = sorted(list(stock))

    '''
    Try a set of alternative names
    '''
    _retry, stock_list = handle_alias(*stock_list,**alias) 
    retry = map(lambda x : alias[x].values(), _retry)
    retry = reduce(lambda a, b : a+b, retry)

    FinanceModel2 = scrape_financemodelling(retry)
    temp_fm = reduce(lambda a, b : a+b, FinanceModel2.values())
    retry = set(retry) - set(temp_fm)
    retry = sorted(list(retry))

    yahoo2 = scrape_yahoo(retry)
    temp_y = reduce(lambda a, b : a+b, yahoo2.values())
    retry = set(retry) - set(temp_y)
    retry = sorted(list(retry))

    logging.info(retry)
    logging.info(temp_fm)
    logging.info(temp_y)
    logging.info(_retry)

    config = INI.init()
    INI.write_section(config,"STOCKMONITOR",**stockmonitor)
    INI.write_section(config,"FINANCEMODEL",**FinanceModel)
    INI.write_section(config,"YAHOO",**yahoo)
    INI.write_section(config,"FINANCEMODEL2",**FinanceModel2)
    INI.write_section(config,"YAHOO2",**yahoo2)
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

