#!/usr/bin/env python

import logging
import pandas as pd
from libBusinessLogic import YAHOO_SCRAPER, ROBUST_PANDAS_FINANCE as PANDAS_FINANCE
from libCommon import LOG_FORMAT_TEST
from libUtils import ENVIRONMENT, exit_on_exception, log_on_exception
from libFinance import TRANSFORM_BACKGROUND
from libDebug import trace

'''
TODO : delete this file , move testing to test suite
'''

@trace
def main(**kwargs) :
    data_store = kwargs.get('data_store',"")
    ticker_list = kwargs.get('ticker_list',[])
    scraper = kwargs.get('scraper',None)
    ret = {}
    for i, ticker in enumerate(ticker_list) :
        filename = '{}/{}.pkl'.format(data_store,ticker)
        scraper['ticker'] = ticker
        prices = PANDAS_FINANCE.SAFE(filename,scraper)
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

