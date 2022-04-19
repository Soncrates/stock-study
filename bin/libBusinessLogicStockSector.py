# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 23:55:10 2022

@author: emers
"""
import logging as log
from time import sleep
import re

from libCommon import dict_append_list
from libUtils import WEB as WEB_UTIL
from libWeb import CONSTANTS

class TRANSFORM_SECTOR() :
    sector_set = ['Basic Materials', 'Consumer Defensive', 'Consumer Cyclical'
                 ,'Communication Services', 'Energy', 'Financial Services'
                 ,'Healthcare','Industrials','Real Estate'
                 ,'Utilities','Technology'
                 ]
    @classmethod
    def normalize(cls,name) :
        test = name.lower()
        if 'estate' in test :
            return 'Real Estate'
        if 'basic' in test :
            return 'Basic Materials'
        if 'utilities' in test :
            return 'Utilities'
        if 'communication' in test :
            return 'Communication Services'
        if 'defensive' in test :
            return 'Consumer Defensive'
        if 'cyclical' in test :
            return 'Consumer Cyclical'
        if 'energy' in test :
            return 'Energy'
        if 'technology' in test :
            return 'Technology'
        if 'healthcare' in test :
            return 'Healthcare'
        if 'industrials' in test :
            return 'Industrials'
        if 'financial' in test :
            return 'Financial Services'
        if 'Industrial Goods' in test :
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
          if not soup :
              return None
          data = [ span.text for span in soup.body.findAll('span') ]
          profile = cls.parse(data,ticker)
          sector = { key:value for (key,value) in profile.items() if 'sector' in key.lower() }
          if not sector or len(sector) == 0:
             log.warning('Sector info unavailable for {}'.format(ticker))
             return None
          log.debug(sector)
          sector = list(sector.values())[0]
          if not sector or len(sector) == 0:
             log.warning('Sector info unavailable for {}'.format(ticker))
             return None
          if sector not in recognized :
             sector = TRANSFORM_SECTOR.normalize(sector)
          log.info("Stock {} belongs in sector {}".format(ticker,sector))
          return sector
      @classmethod
      def extract(cls, stock_list,recognized) :
          log.info((cls, len(stock_list),sorted(stock_list)[:10]))
          ret = {}
          for ticker in stock_list :
              sector = cls.findSector(ticker,recognized)
              dict_append_list(ret, sector,ticker)
          log.info(ret)
          found_list = []
          for v in list(ret.values()) :
              found_list.extend(v)
          log.info('Found {}'.format(len(found_list)))
          return ret, found_list

class FINANCEMODELLING_STOCK_LIST() :
      url = "https://financialmodelingprep.com/api/v3/company/stock/list"
      @classmethod
      def get(cls) :
          response = WEB_UTIL.get_json(cls.url)
          if not response :
              log.warning('No response from {}'.format(cls.url))
              return None
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
      def extract(cls, stock_list) :
          log.info((cls,len(stock_list),sorted(stock_list)[:10]))
          # FINANCEMODELLING_STOCK_LIST calls https://financialmodelingprep.com/api/v3/company/stock/list, which is now forbidden
          #_stock_list = map(lambda x : x.get('symbol', None), FINANCEMODELLING_STOCK_LIST.get())
          #stock_list = set(_stock_list).intersection(set(stock_list))
          stock_list = sorted(list(stock_list))

          ret = {}
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
              dict_append_list(ret,sector,row)
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
      default_headers = { 'Host' : 'www.stockmonitor.com'
                        , 'User-Agent' : 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:75.0) Gecko/20100101 Firefox/75.0'
                        , 'Accept' : 'text/css,*/*;q=0.1'
                        , 'Accept-Language' : 'en-US,en;q=0.5'
                        , 'Accept-Encoding' : 'gzip, deflate, br'
                        , 'Connection' : 'keep-alive'
                        }
      
      def __init__(self, url_list, headers) :
          self.url_list = url_list
          self.headers = headers
      def __str__(self) :
          return type(self)

      @classmethod
      def init(cls,headers) :
          key_list = map(lambda x : x.replace('-','_'), cls.url_list)
          key_list = [ TRANSFORM_SECTOR.normalize(x) for x in cls.url_list ]
          url_list = [ cls.url.format(x) for x in cls.url_list]
          url_list = dict(zip(key_list,url_list))
          ret = cls(url_list,headers)
          msg = ret.url_list.values()
          msg = '\n'.join(sorted(msg))
          log.info(msg)
          log.info(ret.headers)
          return ret
      @classmethod
      def business_logic(cls,sector, url,headers) :
          response = WEB_UTIL.get_text(url, headers=headers)
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
          log.info((sector,len(ret),ret[:10]))
          return ret
      @classmethod
      def extract(cls,headers) :
          obj = STOCKMONITOR.init(headers)
          ret = {sector : obj.business_logic(sector,url,obj.headers) for (sector,url) in obj.url_list.items() }
          found_list = []
          for v in list(ret.values()) :
              found_list.extend(v)
          found_set = sorted(list(set(found_list)))
          if len(found_list) != len(found_set) :
              log.warning("Some stocks in multiple sectors")
          log.info('Found {}'.format(len(found_set)))
          return ret, found_set
              
def handle_alias(stock_list,alias_list) :
    ret = [ stock for stock in alias_list if stock not in stock_list]
    if not ret or len(ret) == 0 :
        log.warning("No aliases found in remaining stock list")
        return [], stock_list
    left_overs = set(stock_list) - set(ret)
    ret = sorted(ret)
    left_overs = sorted(list(left_overs))
    log.info(ret)
    log.debug(left_overs)
    return ret, left_overs   
      
def dep_EXTRACT_SECTOR(stock_names, alias,recognized,headers):

    sm, found_list = STOCKMONITOR.extract(headers)
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

def EXTRACT_SECTOR(stock_list, alias_list,sector_set,headers):

    sm, found_list = STOCKMONITOR.extract(headers)
 
    outdated_list  = [ stock for stock in found_list if stock not in stock_list ]
    if len(outdated_list) > 0 :
       log.warning(("Stocks no longer in NASDAQ",len(outdated_list),outdated_list))

    remaining_stock_list = [stock for stock in stock_list if stock not in found_list]        
    y, found_list = YAHOO.extract(remaining_stock_list,sector_set)
    remaining_stock_list = [stock for stock in remaining_stock_list if stock not in found_list]

    '''
    Try a set of alternative names
    '''
    retry_list = []
    _rety_list = [ stock for stock in alias_list if stock not in remaining_stock_list]
    if len(_rety_list) == 0 :
        log.warning("No aliases found in remaining stock list")
    else :
        for stock in _rety_list :
            retry_list.extend(alias_list[stock])

    y2, found_list = YAHOO.extract(retry_list,sector_set)
    retry_list = [ stock for stock in retry_list if stock not in found_list]
    if len(retry_list) > 0 :
       log.warning((len(retry_list),retry_list[:10]))

    unknown_list = [ stock for stock in remaining_stock_list if stock not in _rety_list]
    nasdaq = {'unknown' : unknown_list }
    if len(retry_list) > 0 :
        nasdaq['alias'] = retry_list 
        
    ret = { "STOCKMONITOR" : sm , 'alias' : alias_list, "NASDAQTRADER" : nasdaq}
    if len(y) > 0 :
       ret["YAHOO"] = y
    if len(y2) > 0 :
       ret["YAHOO2"] = y2
    return ret 