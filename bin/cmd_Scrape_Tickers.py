#!/usr/bin/env python

import logging as log
import time
import os
from libCommon import find_subset
from libUtils import ENVIRONMENT, mkdir
from libNASDAQ import NASDAQ
from libFinance import STOCK_TIMESERIES
from libDecorators import exit_on_exception, singleton
from libDebug import trace


"""
1) pull all stocks and funds (tickers) from NASDAQ
2) use pandas interface to scrape historical price data
3) save to local directories
"""


@singleton
class VARIABLES() :
    var_names = ['env','data_store_stock', 'data_store_fund','wait_on_success','wait_on_failure']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*VARIABLES.var_names))

        mkdir(self.data_store_stock)
        mkdir(self.data_store_fund)

class LOAD() :
      @classmethod
      @trace
      def _prices(cls, wait_on_failure, local_dir, ticker,dud) :
          if dud is None :
             dud = []
          reader = STOCK_TIMESERIES.init()
          prices = reader.extract_from_yahoo(ticker)
          if prices is None :
             dud.append(ticker)
             time.sleep(wait_on_failure)
             return dud
          filename = '{}/{}.pkl'.format(local_dir,ticker)
          filename = os.path.abspath(filename)
          STOCK_TIMESERIES.save(filename, ticker, prices)
          del prices
          return dud

      @classmethod
      def prices(cls,local_dir, wait_on_success, wait_on_failure, ticker_list) :
          dud = None
          total = len(ticker_list)
          for i, ticker in enumerate(ticker_list) :
              log.info("{} ({}/{})".format(ticker,i,total))
              dud = cls._prices(wait_on_failure, local_dir, ticker,dud)
              time.sleep(wait_on_success)
          size = len(ticker_list) - len(dud)
          log.info("Total {}".format(size))
          if len(dud) > 0 :
             dud = sorted(dud)
             log.warning((len(dud),dud))
          return dud
      @classmethod
      @trace
      def robust(cls,data_store, wait_on_success, wait_on_failure, ticker_list) :
          retry = cls.prices(data_store, wait_on_success, wait_on_failure, ticker_list)
          if len(retry) > 0 :
             retry = cls.prices(data_store, wait_on_success, wait_on_failure, retry)
          if len(retry) > 0 :
             log.error((len(retry), sorted(retry)))

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
    log.info(alias)
    return fund_list,stock_list, etf_list, alias

@exit_on_exception
@trace
def main() : 
    fund_list,stock_list, etf_list, alias = get_tickers()

    wait_on_success = VARIABLES().wait_on_success
    wait_on_failure = VARIABLES().wait_on_failure
    LOAD.robust(VARIABLES().data_store_stock, wait_on_success, wait_on_failure, stock_list)
    LOAD.robust(VARIABLES().data_store_stock, wait_on_success, wait_on_failure, etf_list)
    LOAD.robust(VARIABLES().data_store_fund,  wait_on_success, wait_on_failure, fund_list)

if __name__ == '__main__' :
   import sys
   import log
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   log.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=log.INFO)
   #log.basicConfig(stream=sys.stdout, format=log_msg, level=log.DEBUG)

   data_store_stock = '{}/local/historical_prices'.format(env.pwd_parent)
   data_store_fund = '{}/local/historical_prices_fund'.format(env.pwd_parent)
   wait_on_success=0.1
   wait_on_failure=1
   main()

