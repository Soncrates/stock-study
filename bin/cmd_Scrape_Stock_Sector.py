#!/usr/bin/env python

import re
import logging
from functools import reduce
from libCommon import INI, exit_on_exception
from libNASDAQ import NASDAQ, NASDAQ_TRANSFORM
from libWeb import WEB_UTIL
from libDebug import trace
'''
   Web Scraper
   Use RESTful interface to to get web pages and parse for relevant info about stocks and funds
'''
class EXTRACT() :
    _singleton = None
    def __init__(self, _env, draft,final) :
        self.env = _env
        self.draft = draft
        self.final = final
    @classmethod
    def instance(cls) :
        if not (cls._singleton is None) :
           return cls._singleton
        target = 'env'
        env = globals().get(target,None)
        target = 'draft'
        draft = globals().get(target,None)
        target = 'final'
        final = globals().get(target,None)

        cls._singleton = cls(env,draft,final)
        return cls._singleton

class DICT_HELPER() :
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
        if not isinstance(msg,list) :
           msg = list(msg)
        msg.append("Total : {}".format(len(self.values())))
        return "\n".join(msg)
    def append(self, key, *value_list):
        if key not in self.data:
           self.data[key] = []
        for value in value_list :
            self.data[key].append(value)
    def values(self):
        ret = reduce(lambda a, b : a+b, self.data.values())
        ret = sorted(list(set(ret)))
        return ret
    @classmethod
    def init(cls, *largs, **kwargs) :
        ret = cls(*largs, **kwargs)
        return ret
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
              sector = DICT_HELPER.normalize(sector)
              ret.append(sector,row)
          logging.info(ret)
          _stock_list = ret.values()
          return ret.data, _stock_list

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
              sector = DICT_HELPER.normalize(sector)
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
      def __init__(self, url_list) :
          self.url_list = url_list
      def __str__(self) :
          return type(self)

      @classmethod
      def init(cls) :
          key_list = map(lambda x : x.replace('-','_'), cls.url_list)
          key_list = map(lambda x : DICT_HELPER.normalize(x), cls.url_list)
          url_list = map(lambda x : cls.url.format(x), cls.url_list)
          url_list = dict(zip(key_list,url_list))
          ret = cls(url_list)
          msg = ret.url_list.values()
          msg = '\n'.join(sorted(msg))
          logging.info(msg)
          return ret
      @classmethod
      def get(cls,url) :
          response = WEB_UTIL.invoke_url(url)
          soup = WEB_UTIL.format_as_soup(response)
          ret = set([])
          for link in soup.findAll('a', attrs={'href': re.compile("^/quote")}):
              ret.add( link.contents[0] )
          return sorted(list(ret))
      @classmethod
      @trace
      def extract(cls) :
          logging.info("start {}".format(cls))
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
    retry = reduce(lambda a, b : a+b, retry)
    return ret, retry, left_overs 

class TRANSFORM() :
      @classmethod
      def _validate(cls, entry) :
          flag_1 = 'Symbol' in entry and 'Security Name' in entry
          flag_2 = 'NASDAQ Symbol' in entry and 'Security Name' in entry
          flag = flag_1 or flag_2
          return flag
      @classmethod
      def validate(cls, data) :
          data = filter(lambda x : cls._validate(x), data)
          return list(data)
      @classmethod
      def safe(cls, name) :
          name = name.replace('%', ' percent')
          return name
      @classmethod
      def symbol(cls, entry) :
          ret = entry.get('Symbol',None)
          if ret is None :
             ret = entry.get('NASDAQ Symbol',None)
          return ret
      @classmethod
      def to_dict(cls,stock_list, data) :
          ret = {}
          for i, ticker in enumerate(stock_list) :
              if '=' in ticker :
                  continue
              entry = filter(lambda x : cls.symbol(x) == ticker, data)
              entry = list(entry)
              if len(entry) == 0 :
                 continue
              entry = entry[0]
              ret[ticker] = cls.safe(entry['Security Name'])
          return ret
      @classmethod
      def merge(cls) :
          config = EXTRACT.instance().draft
          logging.info('loading config files {}'.format(config))
          ret = DICT_HELPER.init()
          for path, section, key, stock in INI.loadList(*[config]) :
              ret.append(key,*stock)
          omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']
          for omit in omit_list :
              ret.data.pop(omit,None)

          stock_list = ret.values()
          return ret.data, stock_list

class LOAD() :
      @classmethod
      def draft(cls,data) :
          save_file = EXTRACT.instance().draft
          logging.info('Loading results : {}'.format(save_file))
          config = INI.init()
          for i, SECTION in enumerate(sorted(data)) :
              target = 'alias'
              if SECTION == target :
                  alias = data.get(SECTION)
                  continue
              logging.info((i,SECTION))
              value = data.get(SECTION)
              INI.write_section(config, SECTION, **value)
          for name in sorted(alias) :
              INI.write_section(config,name,**alias[name])
          config.write(open(save_file, 'w'))
      @classmethod
      def final(cls,data) :
          save_file = EXTRACT.instance().final
          logging.info('Loading results : {}'.format(save_file))
          config = INI.init()
          INI.write_section(config,"MERGED",**data)
          config.write(open(save_file, 'w'))

def extract():

    nasdaq = NASDAQ.init()

    stock_list, etf_list, alias = nasdaq.stock_list()

    sm, stocks = STOCKMONITOR.extract()
    stock_list = set(stock_list) - set(stocks)
    stock_list = sorted(list(stock_list))

    fm, stocks = FINANCEMODELLING.extract(stock_list)
    stock_list = set(stock_list) - set(stocks)
    stock_list = sorted(list(stock_list))

    y, stocks = YAHOO.extract(stock_list)
    stock_list = set(stock_list) - set(stocks)
    stock_list = sorted(list(stock_list))

    '''
    Try a set of alternative names
    '''
    _retry, retry, stock_list = handle_alias(*stock_list,**alias) 

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
    draft = extract()
    LOAD.draft(draft)
    final, stock_list = TRANSFORM.merge()
    LOAD.final(final)

if __name__ == '__main__' :

   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   draft = '{}/local/stock_by_sector_draft.ini'.format(env.pwd_parent)
   final = '{}/local/stock_by_sector.ini'.format(env.pwd_parent)

   main()
