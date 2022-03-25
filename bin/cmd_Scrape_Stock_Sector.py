#!/usr/bin/env python

import re
import logging as log
from time import sleep
from functools import reduce
from libCommon import INI_READ, INI_WRITE, find_subset
from libUtils import DICT_HELPER, WEB as WEB_UTIL
from libNASDAQ import NASDAQ
from libDecorators import exit_on_exception, singleton
from libDebug import trace
from libWeb import CONSTANTS
'''
   Web Scraper
   Use RESTful interface to to get web pages and parse for relevant info about stocks and funds
'''

@singleton
class VARIABLES() :
    var_names = ['env','draft','final','omit_list','save_file','sector_enum','header']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*VARIABLES.var_names))


class TRANSFORM_SECTOR() :

    @classmethod
    def normalize(cls,name) :
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
        log.warning(name)
        return name

class YAHOO() :
      url = "https://finance.yahoo.com/quote/{0}/profile?p={0}"
      @classmethod
      def get(cls, ticker) :
          url = cls.url.format(ticker)
          ret = WEB_UTIL.get_content(url,CONSTANTS.YAHOO_HEADERS)
          if ret is None :
             sleep(3)
             ret = WEB_UTIL.get_content(url,CONSTANTS.YAHOO_HEADERS)
          return ret
      @classmethod
      def businesslogic1(cls, *data) :
          while True :
                if len(data) == 0 :
                    break
                if 'SECTOR' in data[0].upper() :
                   break
                if 'CATEGORY' in data[0].upper() :
                   break
                if 'INDUSTRY' in data[0].upper() :
                   break
                data = data[1:]
          return data
      @classmethod
      def businesslogic2(cls, key, value) :
          if value=='N/A' : 
              return False
          if key==value :
              return False
          if len(key) == 0 :
              return False
          if key[0] == '1' :
              return False
          return True
      @classmethod
      def parse(cls, data, ticker) :
          if len(data) == 0 :
             log.warning("Could not find spans in html {}".format(ticker)  )
             return {}
          _ret = cls.businesslogic1(*data)
          if len(_ret) == 0 :
             log.warning("Could not find Sector or category or industry for {}".format(ticker)  )
             return {}
          log.debug(_ret)
          key_list = _ret[0::2]
          value_list = _ret[1::2]
          ret = dict(zip(key_list,value_list))
          ret = { key:value for (key,value) in ret.items() if cls.businesslogic2(key,value) }
          log.debug(ret)
          return ret
      @classmethod
      def _extract(cls,ticker) :
          response = cls.get(ticker)
          ret = WEB_UTIL.format_as_soup(response)
          if ret is None : 
             log.warning("No response for {}".format(ticker) )
             return None
          if ret.body is None : 
             log.warning("No body for {}".format(ticker) )
             return None
          return ret
      @classmethod
      def findSector(cls,ticker,recognized) :
          soup = cls._extract(ticker)
          data = [ span.text for span in soup.body.findAll('span') ]
          background = cls.parse(data,ticker)
          sector = background.get('Sector',None) 
          if not sector or len(sector) == 0:
             return None
          if sector not in recognized :
             sector = TRANSFORM_SECTOR.normalize(sector)
          log.info("Stock {} belongs in sector {}".format(ticker,sector))
          return sector
      @classmethod
      @trace
      def extract(cls, stock_list,recognized) :
          log.info((cls, len(stock_list),sorted(stock_list)[:10]))
          ret = DICT_HELPER.init()
          for ticker in stock_list :
              sector = cls.findSector(ticker,recognized)
              if sector is None :
                  continue
              ret.append(sector,ticker)
          log.info(ret)
          _stock_list = ret.values()
          return ret.data, _stock_list

class FINANCEMODELLING_STOCK_LIST() :
      url = "https://financialmodelingprep.com/api/v3/company/stock/list"
      @classmethod
      def get(cls) :
          response = WEB_UTIL.get_json(cls.url)
          target = "symbolsList"
          ret = response.get(target,{})
          log.debug(ret)
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
          log.debug(ret)
          return ret
      @classmethod
      @trace
      def extract(cls, stock_list) :
          log.info((cls,len(stock_list),sorted(stock_list)[:10]))
          # FINANCEMODELLING_STOCK_LIST calls https://financialmodelingprep.com/api/v3/company/stock/list, which is now forbidden
          #_stock_list = map(lambda x : x.get('symbol', None), FINANCEMODELLING_STOCK_LIST.get())
          #stock_list = set(_stock_list).intersection(set(stock_list))
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
          log.info(ret)
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
          key_list = [ TRANSFORM_SECTOR.normalize(x) for x in cls.url_list ]
          url_list = [ cls.url.format(x) for x in cls.url_list]
          url_list = dict(zip(key_list,url_list))
          ret = cls(url_list)
          msg = ret.url_list.values()
          msg = '\n'.join(sorted(msg))
          log.info(msg)
          return ret
      @classmethod
      def business_logic(cls,url) :
          response = WEB_UTIL.get_text(url, headers=VARIABLES().header)
          soup = WEB_UTIL.format_as_soup(response)
          if soup is None : 
             log.warning("No response for {}".format(url) )
             return []
          if soup.body is None : 
             log.warning("No body for {}".format(url) )
             return []
          link_list = soup.findAll('a', attrs={'href': re.compile("^/quote")})
          if not link_list :
             log.warning("No href for {}".format(url) )
             return []
          ret = sorted(list(set([link.contents[0] for link in link_list])))
          log.info((len(ret),ret))
          return ret
      @classmethod
      @trace
      def extract(cls) :
          obj = STOCKMONITOR.init()
          ret = {key : cls.business_logic(value) for (key,value) in obj.url_list.items() }
          found_list = []
          for v in ret.values() :
              found_list.extend(v)
          log.info('Found {}'.format(len(found_list)))
          return ret, found_list

def handle_alias_retry(ret,alias) :
    retry = [ list(set(alias.get(x,[]))) for x in ret ]
    if len(retry) > 0 :
       retry = reduce(lambda a, b : a+b, retry)
    return list(retry)

def handle_alias(*stock_list,**alias) :
    ret = set(alias.keys()).intersection(set(stock_list))
    left_overs = set(stock_list) - ret
    ret = sorted(list(ret))
    left_overs = sorted(list(left_overs))
    retry = handle_alias_retry(ret,alias)
    log.info(ret)
    log.debug(retry)
    log.debug(left_overs)
    return ret, retry, left_overs 

class TRANSFORM() :
      @classmethod
      def merge(cls) :
          ret = DICT_HELPER.init()
          for section, key, stock in INI_READ.read(*[VARIABLES().draft]) :
              ret.append(key,*stock)
          for omit in VARIABLES().omit_list :
              ret.data.pop(omit,None)

          stock_list = ret.values()
          return ret.data, stock_list

class LOAD() :
      @classmethod
      def draft(cls,data) :
          INI_WRITE.write(format(VARIABLES().draft), **data)
      @classmethod
      def final(cls,data) :
          INI_WRITE.write(VARIABLES().final,**{'MERGED' : data})

def business_logic(data_pd) :
    data = data_pd.to_dict()
    ret = {}
    for ticker_list in data.values() :
        log.info(ticker_list)
        for ticker, alias in ticker_list.items() :
            log.debug((ticker,alias))
            if ticker in ret :
                v = ret[ticker]
                v.append(alias)
                ret[ticker] = v
            else :
                ret[ticker] = [alias]
    return ret
def get_tickers() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, alias_pd = nasdaq.stock_list()
    stock_names = stock_list.index.values.tolist()
    log.debug((len(stock_names),stock_names[:10]))
    alias_list = business_logic(alias_pd)
    log.info((len(alias_list),alias_list))
    
    return stock_names, alias_list

def action(stock_names, alias,recognized):

    sm, found_list = STOCKMONITOR.extract()
    stock_names = set(stock_names) - set(found_list)
    stock_names = sorted(list(stock_names))
    #stock_names = stock_names[:30]

    fm = None
    # https://financialmodelingprep.com/api/v3/company/profile/{0}
    # now charges a fee
    #fm, found_list = FINANCEMODELLING.extract(stock_names)
    #stock_names = set(stock_names) - set(found_list)
    #stock_names = sorted(list(stock_names))

    y, found_list = YAHOO.extract(stock_names,recognized)
    stock_names = set(stock_names) - set(found_list)
    stock_names = sorted(list(stock_names))

    '''
    Try a set of alternative names
    '''
    _retry, retry, unknown_list = handle_alias(*stock_names,**alias) 

    fm2 = None
    # https://financialmodelingprep.com/api/v3/company/profile/{0}
    # now charges a fee
    #fm2, found_list = FINANCEMODELLING.extract(retry)
    #retry = set(retry) - set(found_list)
    #retry = sorted(list(retry))

    y2, found_list = YAHOO.extract(retry,recognized)
    retry = set(retry) - set(found_list)
    retry = sorted(list(retry))

    log.info(retry)
    log.info(_retry)

    ret = { "STOCKMONITOR" : sm, "FINANCEMODEL" : fm, "YAHOO" : y
          , "FINANCEMODEL2" : fm2, "YAHOO2" : y2 }
    ret = { "STOCKMONITOR" : sm, "YAHOO" : y
          , "YAHOO2" : y2 }
    ret["NASDAQTRADER"] = {'unknown' : unknown_list , 'alias' : retry }
    ret['alias'] = alias
    return ret 

@exit_on_exception
@trace
def main() :
    stock_names, alias = get_tickers()
    draft = action(stock_names,alias,VARIABLES().sector_enum)
    #alias = draft.pop('alias',{})
    data = { key : sorted(value) for (key,value) in draft.items() }
    data.update(alias)
    LOAD.draft(draft)
    final, stock_list = TRANSFORM.merge()
    LOAD.final(final)

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = ' %(levelname)s %(module)s.%(funcName)s(%(lineno)s) - %(message)s'
   log.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=log.INFO)

   draft = '{}/local/stock_by_sector_draft.ini'.format(env.pwd_parent)
   final = '{}/local/stock_by_sector.ini'.format(env.pwd_parent)
   save_file = '{}/local/stock_background.ini'.format(env.pwd_parent)
   omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']
   sector_enum = ['Basic Materials', 'Consumer Defensive', 'Consumer Cyclical'
                 ,'Communication Services', 'Energy', 'Financial Services'
                 ,'Healthcare','Industrials','Real Estate'
                 ,'Utilities','Technology'
                 ]
   header = { 'Host' : 'www.stockmonitor.com'
            , 'User-Agent' : 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0'
            , 'Accept' : 'text/css,*/*;q=0.1'
            , 'Accept-Language' : 'en-US,en;q=0.5'
            , 'Accept-Encoding' : 'gzip, deflate, br'
            , 'Connection' : 'keep-alive'
            }

   main()
