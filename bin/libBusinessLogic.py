"""
Created on Fri Apr  1 13:59:31 2022
 -*- coding: utf-8 -*-

@author: emers
"""
from copy import deepcopy
import datetime
from json import dumps, load
import logging as log
import os
import time
import pandas as PD
import pandas_datareader as WEB

from libCommon import iterate_config, load_config, CF
from libUtils import log_on_exception, exit_on_exception
from libNASDAQ import NASDAQ

def dump_ticker_name(ret) :
    if not isinstance(ret,str) :
       return ret
    return ret.replace('%', '_pct_').replace('=', '_eq_')
def load_ticker_name(ret) :
    if not isinstance(ret,str) :
       return ret
    return ret.replace('_pct_','%').replace('_eq_','=')
def pre_load_ticker_name(ret) :
    ret = ret.strip()
    if ret.startswith('{') and ret.endswith('}') :
       return load(ret.replace("'",'"').replace("`","'"))
    return [ arg.strip() for arg in ret.split(',') ]
def pre_dump_ticker_name(ret) :
    if isinstance(ret,list) :
       return ",".join(ret)
    if isinstance(ret,dict) :
       return dumps(ret)
    if isinstance(ret,str) :
       return ret.replace("'","`").replace('"',"'")
    return str(ret)

class INI_BASE(object) :
      @classmethod
      def init(cls) :
          ret = CF()
          ret.optionxform=str
          return ret

class INI_READ(object) :
      @classmethod
      def read(cls, *file_list) :
          config_list = [ load_config(arg) for arg in file_list]
          for x, config in enumerate(config_list) :
              for i,j, section, key, value in iterate_config(config) :
                  key, value = cls.transform(key, value)
                  yield section, key, value
      @classmethod
      def transform(cls, key,value) :
            key = load_ticker_name(key)
            value = pre_load_ticker_name(value)
            value = load_ticker_name(value)
            return key, value
                  
class INI_WRITE(object) :
      @classmethod
      def write(cls, filename,**data) :
          filename = filename.replace(' ','SPACE')
          log.info('Writing results : {}'.format(filename))
          log.debug(data)
          config = INI_BASE.init()
          for i,j, section, key, value in iterate_config(data) :
              if not config.has_section(section) :
                  config.add_section(section)
              key, value = cls.transform(key, value)
              config.set(section,key,value)
          fp = open(filename, 'w')
          config.write(fp)
          fp.close()
          log.info('Saved results : {}'.format(filename))
      @classmethod
      def transform(cls, key,value) :
              value = pre_dump_ticker_name(value)
              value = dump_ticker_name(value)
              key = dump_ticker_name(key)
              return key, value

class YAHOO_SCRAPER:
      '''
      Interface into pandas DataReader
      '''
      source = 'yahoo'
      size = 365*10
      date_format = '%Y-%m-%d'
      wait_on_failure = 1
      wait_on_success = 0.1
      @classmethod
      def pandas(cls, **kwargs) :
          '''

          Parameters
          ----------
          kwargs : dict
              end - last day of date range, can be datetime or string format '%Y-%m-%d'
              start -   
          Returns
          -------
          ret : TYPE
              DESCRIPTION.

          '''
          end = kwargs.get('end', datetime.datetime.utcnow())
          if isinstance(end, str) :
             end = datetime.datetime.strptime(end, cls.date_format)
          start = kwargs.get('start', datetime.timedelta(days=cls.size))
          if isinstance(start, str) :
             start = datetime.timedelta(days=int(start))
          start = end - start
          wait_on_failure = kwargs.get('wait_on_failure', cls.wait_on_failure)          
          wait_on_success = kwargs.get('wait_on_success', cls.wait_on_success)          
          ret = { 'start':start, 'end':end, 'source':cls.source, 'wait_on_failure' : wait_on_failure,'wait_on_success' : wait_on_success}
          log.info(ret)
          return ret
class BASE_PANDAS_FINANCE :
      @classmethod
      def make_args(cls, *ticker_list,**kwargs) :
          '''
          Yields
          ------
          args : counter, dict
              parameters for pandas DataReader.
              
          WARNING: del from calling function!!!!!
          '''
          total = len(ticker_list)
          for i, ticker in enumerate(ticker_list) :
              args = deepcopy(kwargs)
              args['ticker'] = ticker
              log.info("{} ({}/{})".format(ticker,i,total))
              yield i, args
              
      @classmethod
      def SAVE(cls, data_store, *ticker_list,**kwargs) :
          dud_list = []
          for i, args in BASE_PANDAS_FINANCE.make_args(*ticker_list, **kwargs) :
              ticker = args['ticker']
              filename = '{}/{}.pkl'.format(data_store,ticker)
              data = ROBUST_PANDAS_FINANCE.EXTRACT(**args)
              if not PANDAS_FINANCE.VALIDATE(ticker, data) :
                  dud_list.append(ticker)
              PANDAS_FINANCE.SAVE(filename,ticker,data)
              del args
          return dud_list
      @classmethod
      def LOAD(cls, data_store, *ticker_list) :
          file_list = { ticker : '{}/{}.pkl'.format(data_store,ticker) for ticker in ticker_list }
          for ticker, filename in file_list.items() :   
              data = ROBUST_PANDAS_FINANCE.LOAD(ticker,filename)
              if not PANDAS_FINANCE.VALIDATE(ticker, data) :
                  continue
              yield ticker, data
class ROBUST_PANDAS_FINANCE :
      @staticmethod
      def EXTRACT(**kwargs) :
          data = PANDAS_FINANCE.EXTRACT(**kwargs)
          ticker = kwargs.get('ticker',None)
          if PANDAS_FINANCE.VALIDATE(ticker, data) :
              wait = kwargs.get('wait_on_success',0.1)
              time.sleep(wait)
              return data
          wait = kwargs.get('wait_on_failure',1)
          time.sleep(wait)
          return None   
      @staticmethod
      def LOAD(ticker,filename) :
        log.debug((ticker,filename))
        name, data = PANDAS_FINANCE.LOAD(filename)
        if ticker == name :
           return data
        if name is None or len(name) == 0 :
            return None
        msg = '{} {}'.format(ticker,name)
        msg = 'ticker does not match between filename and file content {}'.format(msg)
        raise ValueError(msg)          
      @staticmethod
      @log_on_exception
      def SAFE(filename,**kwargs) :
          ticker = kwargs.get('ticker',None)
          filename = os.path.abspath(filename)
          if os.path.exists(filename) :
              return ROBUST_PANDAS_FINANCE.LOAD(ticker, filename)
          data = PANDAS_FINANCE.EXTRACT(**kwargs)
          PANDAS_FINANCE.SAVE(filename, ticker, data)
          return data
class PANDAS_FINANCE :
      @staticmethod
      @log_on_exception
      def EXTRACT(**kwargs) :
          log.info(kwargs)
          ticker = kwargs.get('ticker',None)
          start  = kwargs.get('start',None)
          end    = kwargs.get('end',None)
          source = kwargs.get('source',None)
          return WEB.DataReader(ticker, source, start, end) 
      @staticmethod
      @log_on_exception
      def SAVE(filename, stock, data) :
          if not PANDAS_FINANCE.VALIDATE(stock, data) : 
              return
          filename = os.path.abspath(filename)
          log.info(filename)
          data['Stock'] = stock
          data.to_pickle(filename)
      @staticmethod
      @log_on_exception
      def LOAD(filename) :
          filename = os.path.abspath(filename)
          log.info(filename)
          if not os.path.exists(filename) :
              log.warning("no such file {}".format(filename))
              return None, None
          data = PD.read_pickle(filename)
          name = PANDAS_FINANCE.FIND_NAME(filename, data)
          return name, data
      @staticmethod
      def FIND_NAME(filename, data) :
          target = 'Stock'
          if target in data :
             name = data.pop(target)
             return name[0]
          name = os.path.basename(filename)
          return name.split(".")[0]
      @staticmethod
      def VALIDATE(ticker, data) :
          if data is None or data.empty :
             log.warning("Empty values for {}".format(ticker))
             return False
          log.debug((ticker,sorted(list(data.columns))))
          if 'Adj Close' not in list(data.columns) :
              log.warning("Corrupted read for {}".format(ticker))
              log.warning(data)
              del data
              return False
          return True

class TRANSFORM_TICKER() :
    _prices = 'Adj Close'
    @classmethod
    def get_value(cls, value) :
        if isinstance(value,list) :
           value = value[0]
        if isinstance(value,str) :
           return value
        return 'Unknown'
    @classmethod
    def invert(cls, data) :
        key_list = sorted(data.keys())
        value_list = [ cls.get_value(data[key]) for key in key_list ]
        ret = dict(zip(value_list,key_list))
        return ret
    @classmethod
    def enhance(cls, ret,data) : 
        ret = ret.T
        names = cls.invert(data)
        names = PD.DataFrame([names]).T
        ret['NAME'] = names
        #ret = ret.dropna(thresh=2)
        ret = ret.fillna(-1)
        ret = ret.to_dict()
        log.debug(type(ret))
        return ret

class LOAD_HISTORICAL_DATA() :
    default_column = 'Adj Close'
    def __init__(self, price_list, price_column) :
        self.price_list = price_list
        self.price_column = price_column
    def __repr__(self):
        return f"Historical loader (column:{self.price_column})"
    def act(self, data):
        ticker_list = []
        if isinstance(data,list) :
            ticker_list = deepcopy(data)
        else :
            ticker_list = data.index.values.tolist()
        ret = {}
        for key, name, data in self.load(*ticker_list) :
            ret[key] = data[self.price_column]
        ret = PD.DataFrame(ret)
        ret.fillna(method='bfill', inplace=True)
        log.info(ret.head(3))
        log.info(ret.tail(3))
        return ret
    def load(self, *ticker_list):
        log.info(ticker_list)
        filename_list = [ self.find_file(self.make_suffix(ticker)) for ticker in ticker_list ]
        log.debug((ticker_list,filename_list))
        for i, filename in enumerate(filename_list) :
            if not filename : continue
            name, temp = PANDAS_FINANCE.LOAD(filename)
            yield ticker_list[i], name, temp
    def make_suffix(self, ticker):
        log.debug(ticker)
        return '{}{}.pkl'.format(os.path.sep,ticker.upper())
    def find_file(self, suffix):
        ret = [ x for x in self.price_list if x.endswith(suffix)]
        if len(ret) == 0 :
           log.warning("{} not in {}".format(suffix, self.price_list[:5]))
           return None
        if len(ret) > 1 :
            suffix = os.path.sep + suffix
            ret = [ x for x in ret if x.endswith(suffix)]
        ret = ret[0]
        log.debug(ret)
        return ret        

def filter_alias(data) :
    log.debug(data)
    ret = data.filter(regex="Symbol", axis=1)
    log.debug(ret)
    for column in ret.columns.values.tolist() :
        for key in ret.index.values.tolist() :
            ret = ret[ret[column]!=key]
    log.debug(ret)
    return ret
def transform_alias(data) :
    ret = {}
    key_list = data.index.values.tolist()
    value_list = data.values.tolist()
    for i, key in enumerate(key_list) :
        ret[key] = list(set(value_list[i]))
    return ret
@exit_on_exception
def NASDAQ_EXTRACT() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, other_list = nasdaq.extract_stock_list()
    fund_list, raw = nasdaq.extract_fund_list()
    stock_list = stock_list.index.values.tolist()
    etf_list = etf_list.index.values.tolist()
    fund_list = fund_list.index.values.tolist()

    # remove alias keys that are not in stock_list
    other_list.loc[ other_list.index.isin(stock_list), : ]
    log.info("other_list ({}) {}".format(len(other_list),other_list[:10]))
    alias_list = filter_alias(other_list)
    log.info("aliases ({}) {}".format(len(alias_list),alias_list[:10]))
    alias_list = transform_alias(alias_list)
    
    log.info("Stock ({}) {}".format(len(stock_list),stock_list[:10]))
    log.info("ETF ({}) {}".format(len(etf_list),etf_list[:10]))
    log.info("Funds ({}) {}".format(len(fund_list),fund_list[:10]))
    log.info(('alias',alias_list))
    return {"fund_list" : fund_list,"stock_list"  : stock_list, "etf_list" : etf_list, "alias_list" : alias_list}