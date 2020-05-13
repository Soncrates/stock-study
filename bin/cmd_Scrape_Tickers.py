#!/usr/bin/env python

import logging
import time
from libUtils import ENVIRONMENT, mkdir
from libNASDAQ import NASDAQ
from libFinance import STOCK_TIMESERIES
from libDecorators import exit_on_exception, singleton
from libDebug import trace

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
    var_names = ['env','data_store_stock', 'data_store_fund']
    def __init__(self) :
        values = get_globals(*VARIABLES.var_names)
        self.__dict__.update(**values)

        mkdir(self.data_store_stock)
        mkdir(self.data_store_fund)

class LOAD() :
      @classmethod
      @trace
      def _prices(cls, local_dir, ticker,dud) :
          if dud is None :
             dud = []
          filename = '{}/{}.pkl'.format(local_dir,ticker)
          reader = STOCK_TIMESERIES.init()
          prices = reader.extract_from_yahoo(ticker)
          if prices is None :
             dud.append(ticker)
             time.sleep(4)
             return dud
          STOCK_TIMESERIES.save(filename, ticker, prices)
          del prices
          return dud

      @classmethod
      def prices(cls,local_dir, ticker_list) :
          dud = None
          total = len(ticker_list)
          for i, ticker in enumerate(ticker_list) :
              logging.info("{} ({}/{})".format(ticker,i,total))
              dud = cls._prices(local_dir, ticker,dud)
              time.sleep(.1)
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

def get_tickers() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, _alias = nasdaq.stock_list()
    fund_list = nasdaq.fund_list()
    stock_list = stock_list.index.values.tolist()
    etf_list = etf_list.index.values.tolist()
    fund_list = fund_list.index.values.tolist()
    alias = []
    for column in _alias.columns.values.tolist() :
        alias.extend(_alias[column].tolist())
    alias = sorted(list(set(alias)))
    logging.info(alias)
    return fund_list,stock_list, etf_list, alias

@exit_on_exception
@trace
def main() : 
    fund_list,stock_list, etf_list, alias = get_tickers()

    LOAD.robust(VARIABLES().data_store_stock, stock_list)
    LOAD.robust(VARIABLES().data_store_stock, etf_list)
    LOAD.robust(VARIABLES().data_store_fund, fund_list)

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   data_store_stock = '{}/local/historical_prices'.format(env.pwd_parent)
   data_store_fund = '{}/local/historical_prices_fund'.format(env.pwd_parent)

   main()

