#!/usr/bin/env python

import logging as log
import time
import os
from libBusinessLogic import YAHOO_SCRAPER
from libCommon import find_subset, LOG_FORMAT_TEST
from libUtils import ENVIRONMENT, mkdir
from libNASDAQ import NASDAQ
from libFinance import PANDAS_FINANCE
from libDecorators import exit_on_exception, singleton
from libDebug import trace


"""
1) pull all stocks and funds (tickers) from NASDAQ
2) use pandas interface to scrape historical price data
3) save to local directories
"""

@singleton
class VARIABLES() :
    var_names = ['env','data_store_stock', 'data_store_fund','wait_on_success','wait_on_failure','fund_list','stock_list', 'etf_list', 'alias','scraper']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*VARIABLES.var_names))

class REFRESH() :
      @classmethod
      @trace
      def _prices(cls, dud,**kwargs) :
          log.info(kwargs)
          ticker = kwargs.get('ticker',None)
          if dud is None :
             dud = []
          data = PANDAS_FINANCE.EXTRACT(**kwargs)
          if not PANDAS_FINANCE.VALIDATE(ticker, data) :
             target = 'wait_on_failure'
             wait_on_failure = kwargs.get(target,None)
             time.sleep(wait_on_failure)
             dud.append(ticker)
             return dud
          data_store = kwargs.get('data_store',None)
          filename = '{}/{}.pkl'.format(data_store,ticker)
          PANDAS_FINANCE.SAVE(filename, ticker, data)
          del data
          return dud
      @classmethod
      def prices(cls,**kwargs) :
          wait_on_success = kwargs.pop('wait_on_success',None)
          ticker_list = kwargs.pop('ticker_list',None)
          scraper = kwargs.pop('scraper',{})
          scraper.update({key:value for (key,value) in kwargs.items() if key in ['wait_on_failure','data_store'] })

          dud = None
          for i, args in YAHOO_SCRAPER.make_args(*ticker_list, **scraper) :
              dud = cls._prices(dud,**args)
              del args
              time.sleep(wait_on_success)
          size = len(ticker_list) - len(dud)
          log.info("Total {}".format(size))
          if len(dud) > 0 :
             dud = sorted(dud)
             log.warning((len(dud),dud))
          return dud
      @classmethod
      @trace
      def robust(cls,**kwargs) :
          retry = cls.prices(**kwargs)
          if len(retry) > 0 :
             kwargs['ticker_list'] = retry
             retry = cls.prices(**kwargs)
          if len(retry) > 0 :
             log.error((len(retry), sorted(retry)))

@exit_on_exception
@trace
def get_tickers() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, alias_list = nasdaq.stock_list()
    fund_list = nasdaq.fund_list()
    stock_list = stock_list.index.values.tolist()
    etf_list = etf_list.index.values.tolist()
    fund_list = fund_list.index.values.tolist()
    alias = []
    for column in alias_list.columns.values.tolist() :
        alias.extend(alias_list[column].tolist())
    alias = sorted(list(set(alias)))
    log.info(alias)
    return fund_list,stock_list, etf_list, alias

@exit_on_exception
@trace
def main() : 
    mkdir(VARIABLES().data_store_stock)
    mkdir(VARIABLES().data_store_fund)

    args = find_subset(vars(VARIABLES()),*VARIABLES.var_names)
    args['ticker_list'] = VARIABLES().stock_list
    args['data_store'] = VARIABLES().data_store_stock
    REFRESH.robust(**args)
    args['ticker_list'] = VARIABLES().etf_list
    REFRESH.robust(**args)
    args['ticker_list'] = VARIABLES().fund_list
    args['data_store'] = VARIABLES().data_store_fund
    REFRESH.robust(**args)

if __name__ == '__main__' :
   import sys
   import logging as log
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)
   #log.basicConfig(stream=sys.stdout, format=log_msg, level=log.DEBUG)

   scraper = YAHOO_SCRAPER().pandas()
   fund_list,stock_list, etf_list, alias = get_tickers()

   data_store_stock = '{}/local/historical_prices'.format(env.pwd_parent)
   data_store_fund = '{}/local/historical_prices_fund'.format(env.pwd_parent)
   wait_on_success=0.1
   wait_on_failure=1
   main()

