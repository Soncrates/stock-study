#!/usr/bin/env python

import logging
import time
from libUtils import ENVIRONMENT, exit_on_exception
from libNASDAQ import NASDAQ, NASDAQ_TRANSFORM
from libFinance import STOCK_TIMESERIES
from libDebug import trace

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, stock, fund, reader) :
        self.env = _env
        self.data_store_stock = stock
        self.data_store_fund = fund
        self.reader = reader
    @classmethod
    def instance(cls) :
        if not (cls._singleton is None) :
           return cls._singleton
        target = 'env'
        env = globals().get(target,None)
        target = 'data_store_stock'
        stock = globals().get(target,'')
        if not isinstance(stock,str) :
           stock = str(stock)
        target = 'data_store_fund'
        fund = globals().get(target,'')
        if not isinstance(fund,str) :
           fund = str(fund)
        reader = STOCK_TIMESERIES.init()

        env.mkdir(stock)
        env.mkdir(fund)
        cls._singleton = cls(env,stock,fund,reader)
        return cls._singleton

class LOAD() :
      @classmethod
      @trace
      def _prices(cls, local_dir, ticker,dud) :
          logging.info(ticker)
          if dud is None :
             dud = []
          filename = '{}/{}.pkl'.format(local_dir,ticker)
          reader =  EXTRACT.instance().reader
          prices = reader.extract_from_yahoo(ticker)
          if prices is None :
             dud.append(ticker)
             time.sleep(4)
             return dud
          STOCK_TIMESERIES.save(filename, ticker, prices)
          del prices
          return dud

      @classmethod
      @trace
      def prices(cls,local_dir, ticker_list) :
          dud = None
          total = len(ticker_list)
          for i, ticker in enumerate(ticker_list) :
              dud = cls._prices(local_dir, ticker,dud)
              time.sleep(.3)
          size = len(ticker_list) - len(dud)
          logging.info("Total {}".format(size))
          if len(dud) > 0 :
             dud = sorted(dud)
             logging.warn((len(dud),dud))
          return dud
      @classmethod
      @trace
      def robust(cls,data_store, ticker_list) :
          retry = cls.prices(data_store, ticker_list)
          if len(retry) > 0 :
             retry = cls.prices(data_store, retry)
          if len(retry) > 0 :
             logging.error((len(retry), sorted(retry)))

def init() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, alias = nasdaq.stock_list()
    fund_list = nasdaq.fund_list()
    return fund_list,stock_list, etf_list, alias

@exit_on_exception
@trace
def main() : 
    data_store = EXTRACT.instance().data_store_stock
    fund_list,stock_list, etf_list, alias = init()
    LOAD.robust(data_store, stock_list)
    LOAD.robust(data_store, etf_list)

    fund_list = map(lambda x : NASDAQ_TRANSFORM.fund_ticker(x),fund_list)
    fund_list = list(fund_list)
    data_store = EXTRACT.instance().data_store_fund
    LOAD.robust(data_store, fund_list)

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '{}/historical_prices'.format(local_dir)
   data_store_stock = '../local/historical_prices'
   data_store_fund = '../local/historical_prices_fund'

   main()

