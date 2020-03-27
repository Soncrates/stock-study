#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI, ENVIRONMENT, exit_on_exception, log_on_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libFinance import TRANSFORM_SHARPE as SHARPE, TRANSFORM_CAGR as CAGR
from libFinance import TRANSFORM_DRAWDOWN as DRAWDOWN, TRANSFORM_DAILY as DAILY
from libDebug import trace

class EXTRACT_TICKER() :
      reader = None
      @classmethod
      def save(cls, local_dir, ticker,dud) :
          filename = '{}/{}.pkl'.format(local_dir,ticker)
          if cls.reader is None :
              cls.reader = STOCK_TIMESERIES.init()
          prices = cls.reader.extract_from_yahoo(ticker)
          if prices is None :
             dud.append(ticker)
             return dud
          STOCK_TIMESERIES.save(filename, ticker, prices)
          return dud
      @classmethod
      def save_list(cls,local_dir, ticker_list) :
          dud = []
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
      def load(cls, data_store, ticker) :
          filename = '{}/{}.pkl'.format(data_store,ticker)
          name, data = STOCK_TIMESERIES.load(filename)
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

    @classmethod
    def summary(cls, data) :
        if data is None :
           return pd.DataFrame()
        prices = data[cls._prices]
        ret = SHARPE.find(prices, period=FINANCE.YEAR, span=2*FINANCE.YEAR)
        cagr = CAGR.find(prices)
        ret.update(cagr)
        daily = DAILY.enrich(prices)
        drawdown = DRAWDOWN.find(daily['daily'])
        ret.update(drawdown)
        ret = pd.DataFrame([ret]).T
        logging.debug(ret)
        return ret

class LOAD() :
      @classmethod
      @trace
      def robust(cls,data_store, ticker_list) :
          retry = cls.prices(data_store, ticker_list)
          if len(retry) > 0 :
             retry = cls.prices(data_store, retry)
          if len(retry) > 0 :
             logging.error((len(retry), sorted(retry)))

def process_prices(ticker_list) :
    data_store = EXTRACT.instance().data_store
    ret = []
    for ticker in ticker_list :
        prices = EXTRACT.prices(data_store, ticker)
        summary = TRANSFORM.prices(prices)
        summary.rename(columns={0:ticker},inplace=True)
        ret.append(summary)
    ret = pd.concat(ret, axis=1)
    ret.rename(index={'risk':'RISK','len':'LEN','sharpe':'SHARPE','returns':'RETURNS'},inplace=True)
    logging.info(ret)
    return ret.T.to_dict()

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT


   _XXX = ['RETURNS','RISK','SHARPE','CAGR','MAX DRAWDOWN','MAX INCREASE']
   _XXX = ['RETURNS','RISK','SHARPE','CAGR','MAX DRAWDOWN',]
   _XXX = sorted(_XXX)

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '{}/historical_prices'.format(local_dir)
   data_store = '../local/historical_prices'

   ini_list = env.list_filenames('local/*.ini')

   ticker_list = ['^GSPC','AAPL','RAFGX','SPY']
   ticker_list = ['AAPL','RAFGX','SPY']
   ticker_list = ['SPY','RAFGX','AAPL','^GSPC']
   ret = []
   for ticker in ticker_list :
       prices = EXTRACT_TICKER.load(data_store, ticker)
       summary = TRANSFORM_TICKER.summary(prices)
       summary.rename(columns={0:ticker},inplace=True)
       ret.append(summary)
   ret = pd.concat(ret, axis=1)
   logging.info(ret.loc[_XXX])
   logging.info(ret.T[_XXX])

