#!/usr/bin/env python

import re
import logging
from functools import reduce
from libCommon import INI_BASE, INI_READ, INI_WRITE
from libUtils import DICT_HELPER, WEB as WEB_UTIL
from libNASDAQ import NASDAQ
from libDecorators import exit_on_exception, singleton
from libDebug import trace
'''
   Web Scraper
   Use RESTful interface to to get web pages and parse for relevant info about stocks and funds
'''
def get_globals(*largs) :
    ret = {}
    for name in largs :
        value = globals().get(name,None)
        if value is None :
           continue
        ret[name] = value
    return ret

@singleton
class VARIABLES() :
    var_names = ['env','draft','final','omit_list']
    def __init__(self) :
        values = get_globals(*VARIABLES.var_names)
        self.__dict__.update(**values)

class TRANSFORM_SECTOR() :
    normalized = ['Basic Materials'
              ,'Utilities'
              ,'Real Estate'
              ,'Communication Services'
              ,'Consumer Defensive'
              ,'Consumer Cyclical'
              ,'Energy'
              ,'Technology'
              ,'Healthcare'
              ,'Industrials'
              ,'Financial Services']

    @classmethod
    def normalize(cls,name) :
        if name in cls.normalized :
            return name
        if 'estate' in name :
            return 'Real Estate'
        if 'basic' in name :
            return 'Basic Materials'
        if 'utilities' in name :
            return 'Utilities'
        if 'communication' in name :
            return 'Communication Services'
        if 'defensive' in name :
            return 'Consumer Defensive'
        if 'cyclical' in name :
            return 'Consumer Cyclical'
        if 'energy' in name :
            return 'Energy'
        if 'technology' in name :
            return 'Technology'
        if 'healthcare' in name :
            return 'Healthcare'
        if 'industrials' in name :
            return 'Industrials'
        if 'financial' in name :
            return 'Financial Services'
        if 'Industrial Goods' in name :
            return 'Industrials'
        logging.warn(name)
        return name

class YAHOO() :
      url = "https://finance.yahoo.com/quote/{0}/profile?p={0}"
      @classmethod
      def get(cls, stock) :
          url = cls.url.format(stock)
          response = WEB_UTIL.get_content(url)
          if response is None :
             response = WEB_UTIL.get_content(url)
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
          key_list = data[0::2]
          value_list = data[1::2]
          ret = dict(zip(key_list,value_list))
          ret = { key:value for (key,value) in ret.items() if len(key) > 0 and key[0] != '1'}
          ret = { key:value for (key,value) in ret.items() if key != ret[key]}
          ret = { key:value for (key,value) in ret.items() if ret[key]!='N/A'}

          logging.debug(ret)
          return ret
      @classmethod
      @trace
      def extract(cls, stock_list) :
          logging.info((cls, len(stock_list),sorted(stock_list)[:10]))
          ret = DICT_HELPER.init()
          _sector = 'Sector'
          for row in stock_list :
              logging.info(row)
              stock = cls.get(row)
              sector = stock.get(_sector,None) 
              if not sector :
                 continue
              sector = TRANSFORM_SECTOR.normalize(sector)
              ret.append(sector,row)
          logging.info(ret)
          _stock_list = ret.values()
          return ret.data, _stock_list

class FINANCEMODELLING_STOCK_LIST() :
      url = "https://financialmodelingprep.com/api/v3/company/stock/list"
      @classmethod
      def get(cls) :
          response = WEB_UTIL.get_json(cls.url)
          target = "symbolsList"
          ret = response.get(target,{})
          logging.debug(ret)
          return ret

class FINANCEMODELLING() :
      url = "https://financialmodelingprep.com/api/v3/company/profile/{0}"
      @classmethod
      def get(cls, stock) :
          url = cls.url.format(stock)
          response = WEB_UTIL.get_json(url)
          target = "profile"
          ret = response.get(target,{})
          ret['Stock'] = stock
          logging.debug(ret)
          return ret
      @classmethod
      @trace
      def extract(cls, stock_list) :
          logging.info((cls,len(stock_list),sorted(stock_list)[:10]))
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
              sector = TRANSFORM_SECTOR.normalize(sector)
              ret.append(sector,row)
          logging.info(ret)
          _stock_list = ret.values()
          return ret.data, _stock_list

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
      , 'real-estate'
      , 'technology'
      , 'utilities'
      ]
      header = { 'Host' : 'www.stockmonitor.com'
               , 'User-Agent' : 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0'
               , 'Accept' : 'text/css,*/*;q=0.1'
               , 'Accept-Language' : 'en-US,en;q=0.5'
               , 'Accept-Encoding' : 'gzip, deflate, br'
               , 'Connection' : 'keep-alive'
               }
      def __init__(self, url_list) :
          self.url_list = url_list
      def __str__(self) :
          return type(self)

      @classmethod
      def init(cls) :
          key_list = map(lambda x : x.replace('-','_'), cls.url_list)
          key_list = map(lambda x : TRANSFORM_SECTOR.normalize(x), cls.url_list)
          url_list = map(lambda x : cls.url.format(x), cls.url_list)
          url_list = dict(zip(key_list,url_list))
          ret = cls(url_list)
          msg = ret.url_list.values()
          msg = '\n'.join(sorted(msg))
          logging.info(msg)
          return ret
      @classmethod
      def get(cls,url) :
          response = WEB_UTIL.get_text(url, headers=cls.header)
          soup = WEB_UTIL.format_as_soup(response)
          ret = set([])
          for link in soup.findAll('a', attrs={'href': re.compile("^/quote")}):
              ret.add( link.contents[0] )
          return sorted(list(ret))
      @classmethod
      @trace
      def extract(cls) :
          obj = STOCKMONITOR.init()
          ret = DICT_HELPER.init()
          for sector in sorted(obj.url_list) : 
              logging.debug(sector)
              url = obj.url_list[sector]
              stock_list = cls.get(url)
              ret[sector] = stock_list
          logging.info(ret)
          _stock_list = ret.values()
          return ret.data, _stock_list 

def handle_alias(*stock_list,**alias) :
    ret = set(alias.keys()).intersection(set(stock_list))
    left_overs = set(stock_list) - ret
    ret = sorted(list(ret))
    left_overs = sorted(list(left_overs))
    logging.info(ret)
    retry = map(lambda x : alias[x].values(), ret)
    retry = map(lambda x : list(x), retry)
    if not isinstance(retry,list) :
       retry = list(retry)
    if len(retry) != 0 :
       retry = reduce(lambda a, b : a+b, retry)
    return ret, retry, left_overs 

class TRANSFORM() :
      @classmethod
      def merge(cls) :
          ret = DICT_HELPER.init()
          logging.info('loading config files {}'.format(VARIABLES().draft))
          for path, section, key, stock in INI_READ.read(*[VARIABLES().draft]) :
              ret.append(key,*stock)
          for omit in VARIABLES().omit_list :
              ret.data.pop(omit,None)

          stock_list = ret.values()
          return ret.data, stock_list

class LOAD() :
      @classmethod
      def draft(cls,data) :
          logging.info('Loading results : {}'.format(VARIABLES().draft))
          config = INI_BASE.init()
          for i, SECTION in enumerate(sorted(data)) :
              target = 'alias'
              if SECTION == target :
                  alias = data.get(SECTION)
                  continue
              logging.info((i,SECTION))
              value = data.get(SECTION)
              INI_WRITE.write_section(config, SECTION, **value)
          for name in sorted(alias) :
              INI_WRITE.write_section(config,name,**alias[name])
          config.write(open(VARIABLES().draft, 'w'))
          logging.info('Saved results : {}'.format(VARIABLES().draft))
      @classmethod
      def final(cls,data) :
          logging.info('Loading results : {}'.format(VARIABLES().final))
          INI_WRITE.write(VARIABLES().final,**{'MERGED' : data})

def get_tickers() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, alias = nasdaq.stock_list()
    stock_names = stock_list.index.values.tolist()
    return stock_names, alias

def action(stock_names, alias):

    sm, stocks = STOCKMONITOR.extract()
    stock_names = set(stock_names) - set(stocks)
    stock_names = sorted(list(stock_names))

    fm, stocks = FINANCEMODELLING.extract(stock_names)
    stock_names = set(stock_names) - set(stocks)
    stock_names = sorted(list(stock_names))

    y, stocks = YAHOO.extract(stock_names)
    stock_names = set(stock_names) - set(stocks)
    stock_names = sorted(list(stock_names))

    '''
    Try a set of alternative names
    '''
    _retry, retry, stock_list = handle_alias(*stock_names,**alias) 

    fm2, stocks = FINANCEMODELLING.extract(retry)
    retry = set(retry) - set(stocks)
    retry = sorted(list(retry))

    y2, stocks = YAHOO.extract(retry)
    retry = set(retry) - set(stocks)
    retry = sorted(list(retry))

    logging.info(retry)
    logging.info(_retry)

    ret = { "STOCKMONITOR" : sm, "FINANCEMODEL" : fm, "YAHOO" : y
          , "FINANCEMODEL2" : fm2, "YAHOO2" : y2 }
    ret["NASDAQTRADER"] = {'unknown' : stock_list , 'alias' : retry }
    ret['alias'] = alias
    return ret 

@exit_on_exception
@trace
def main() :
    stock_names, alias = get_tickers()
    draft = action(stock_names,alias)
    LOAD.draft(draft)
    final, stock_list = TRANSFORM.merge()
    LOAD.final(final)

if __name__ == '__main__' :

   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)

   draft = '{}/local/stock_by_sector_draft.ini'.format(env.pwd_parent)
   final = '{}/local/stock_by_sector.ini'.format(env.pwd_parent)
   omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']

   main()
