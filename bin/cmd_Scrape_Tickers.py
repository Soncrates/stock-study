#!/usr/bin/env python

import logging as log
from libBusinessLogic import YAHOO_SCRAPER, BASE_PANDAS_FINANCE, NASDAQ_EXTRACT
from libCommon import LOG_FORMAT_TEST
from libUtils import mkdir
from libDecorators import exit_on_exception
from libDebug import trace


"""
1) pull all stocks and funds (tickers) from NASDAQ
2) use pandas interface to scrape historical price data
3) save to local directories
"""

@trace
def refresh(**kwargs) :
    ticker_list = kwargs.pop('ticker_list',[])
    data_store = kwargs.pop('data_store',[])
    scraper = kwargs.pop('scraper',{})
    retry = BASE_PANDAS_FINANCE.SAVE(data_store, *ticker_list, **scraper)
    if len(retry) > 0 :
       log.warning((len(retry),sorted(retry)))
       retry = BASE_PANDAS_FINANCE.SAVE(data_store, *retry, **scraper)
    if len(retry) > 0 :
       log.error((len(retry), sorted(retry)))

@exit_on_exception
@trace
def main(**args) : 
    log.debug(args)
    mkdir(args['data_store_stock'])
    mkdir(args['data_store_fund'])

    ticker_list = NASDAQ_EXTRACT()
    log.debug(ticker_list['alias_list'])

    args['ticker_list'] = ticker_list['stock_list']
    args['data_store'] = args['data_store_stock']
    refresh(**args)
    args['ticker_list'] = ticker_list['etf_list']
    #refresh(**args)
    args['ticker_list'] = ticker_list['fund_list']
    args['data_store'] =  args['data_store_fund']
    #refresh(**args)

if __name__ == '__main__' :
   import argparse
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)

   data_store_stock = '{}/local/historical_prices'.format(env.pwd_parent)
   data_store_fund = '{}/local/historical_prices_fund'.format(env.pwd_parent)
   
   parser = argparse.ArgumentParser(description='WEb Scrape stock data from YAHOO')
   parser.add_argument('--data_store_stock', action='store', dest='data_store_stock', type=str, default=data_store_stock, help='location of stock data')
   parser.add_argument('--data_store_fund', action='store', dest='data_store_fund', type=str, default=data_store_fund, help='store report meta')

   scraper = YAHOO_SCRAPER().pandas()

   var_names = ['env','data_store_stock', 'data_store_fund','scraper']
   init = { key : value for (key,value) in globals().items() if key in var_names }
   main(**init)

