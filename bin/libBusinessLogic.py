# -*- coding: utf-8 -*-
from copy import deepcopy
import datetime
import logging as log
import os
import time
import pandas as PD
import pandas_datareader as WEB

from libUtils import log_on_exception, exit_on_exception
from libNASDAQ import NASDAQ
"""
Created on Fri Apr  1 13:59:31 2022

@author: emers
"""

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
    
@exit_on_exception
def NASDAQ_EXTRACT() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, alias_list = nasdaq.extract_stock_list()
    fund_list, raw = nasdaq.extract_fund_list()
    stock_list = stock_list.index.values.tolist()
    etf_list = etf_list.index.values.tolist()
    fund_list = fund_list.index.values.tolist()

    log.info("aliases ({}) {}".format(len(alias_list),alias_list[:10]))

    # remove alias keys that are not in stock_list
    alias_list.loc[ alias_list.index.isin(stock_list), : ]
    
    log.info("Stock ({}) {}".format(len(stock_list),stock_list[:10]))
    log.info("ETF ({}) {}".format(len(etf_list),etf_list[:10]))
    log.info("Funds ({}) {}".format(len(fund_list),fund_list[:10]))
    log.info(('alias',alias_list))
    return {"fund_list" : fund_list,"stock_list"  : stock_list, "etf_list" : etf_list, "alias_list" : alias_list}