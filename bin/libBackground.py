#!/usr/bin/env python

import os
import logging
import pandas as pd
from libBusinessLogic import YAHOO_SCRAPER
from libCommon import LOG_FORMAT_TEST
from libUtils import ENVIRONMENT, exit_on_exception, log_on_exception
from libFinance import PANDAS_FINANCE, TRANSFORM_BACKGROUND
from libDebug import trace

class EXTRACT_TICKER() :
      reader = None
      @classmethod
      def read(cls, ticker) :
          if cls.reader is None :
             cls.reader = PANDAS_FINANCE.init()
          return cls.reader.extract_from_yahoo(ticker)
      @classmethod
      def save(cls, local_dir, ticker,dud = None) :
          if dud is None :
             dud = []
          data = cls.read(ticker)
          if data is None :
             dud.append(ticker)
             return dud
          filename = '{}/{}.pkl'.format(local_dir,ticker)
          PANDAS_FINANCE.SAVE(filename, ticker, data)
          return dud
      @classmethod
      def save_list(cls,local_dir, ticker_list) :
          dud = None
          for ticker in ticker_list :
              dud = cls.save(local_dir, ticker,dud)
          size = len(ticker_list) - len(dud)
          logging.info("Total {}".format(size))
          if len(dud) > 0 :
             dud = sorted(dud)
             logging.warn((len(dud),dud))
          return dud
      @classmethod
      @log_on_exception
      def dep_load(cls, data_store, ticker) :
          filename = '{}/{}.pkl'.format(data_store,ticker)
          logging.debug(filename)
          if not os.path.exists(filename) :
             data = cls.read(ticker)
             PANDAS_FINANCE.SAVE(filename, ticker, data)
             return data
          name, data = PANDAS_FINANCE.LOAD(filename)
          if ticker == name :
             return data
          msg = '{} {}'.format(ticker,name)
          msg = 'ticker does not match between filename and file content {}'.format(msg)
          raise ValueError(msg)

class TRANSFORM_TICKER() :
    _prices = 'Adj Close'
    @classmethod
    def _data(cls, value) :
        if isinstance(value,list) :
           value = value[0]
        if isinstance(value,str) :
           return value
        return 'Unknown'
    @classmethod
    def data(cls, data) :
        key_list = sorted(data.keys())
        value_list = map(lambda key : data[key], key_list)
        value_list = map(lambda key : cls._data(key), value_list)
        ret = dict(zip(value_list,key_list))
        return ret

@trace
def dep_load(**kwargs) :
    target = 'data_store'
    data_store = kwargs.get(target,"")
    target = 'ticker_list'
    ticker_list = kwargs.get(target,[])
    retry = EXTRACT_TICKER.save_list(data_store, ticker_list)
    if len(retry) > 0 :
       retry = EXTRACT_TICKER.save_list(data_store, retry)
    if len(retry) > 0 :
       logging.error((len(retry), sorted(retry)))

@trace
def main(**kwargs) :
    data_store = kwargs.get('data_store',"")
    ticker_list = kwargs.get('ticker_list',[])
    scraper = kwargs.get('scraper',None)
    ret = {}
    for i, ticker in enumerate(ticker_list) :
        filename = '{}/{}.pkl'.format(data_store,ticker)
        scraper['ticker'] = ticker
        prices = PANDAS_FINANCE.ROBUST(filename,scraper)
        ret[ticker] = TRANSFORM_BACKGROUND.find(prices)
    ret = pd.DataFrame(ret)
    logging.debug(ret)
    return ret

if __name__ == '__main__' :
   import sys

   _XXX = ['RETURNS','RISK','SHARPE','CAGR','MAX DRAWDOWN','MAX INCREASE']
   _XXX = ['RETURNS','RISK','SHARPE','CAGR','MAX DRAWDOWN',]
   _XXX = sorted(_XXX)

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=LOG_FORMAT_TEST, level=logging.INFO)

   scraper = YAHOO_SCRAPER.pandas()
   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '{}/historical_prices'.format(local_dir)
   data_store = '../local/historical_prices'

   ini_list = env.list_filenames('local/*.ini')

   ticker_list = ['^GSPC','AAPL','RAFGX','SPY']
   ticker_list = ['AAPL','RAFGX','SPY']
   ticker_list = ['SPY','RAFGX','AAPL','^GSPC']
   ret = main(data_store=data_store, ticker_list=ticker_list,scraper=scraper)
   logging.info(ret.loc[_XXX])
   logging.info(ret.T[_XXX])

