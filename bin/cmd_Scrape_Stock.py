#!/usr/bin/env python

import sys,logging

import pandas as pd
from libCommon import INI_BASE, INI_WRITE
from libNASDAQ import NASDAQ
from libBackground import EXTRACT_TICKER
from libFinance import TRANSFORM_BACKGROUND
from libDecorators import singleton, exit_on_exception, log_on_exception
from libDebug import trace, debug_object

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
    var_names = ['env','save_file',"data_store"]
    def __init__(self) :
        values = get_globals(*VARIABLES.var_names)
        self.__dict__.update(**values)
        debug_object(self)

def get_tickers() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, alias = nasdaq.stock_list()
    ret = stock_list.append(etf_list)
    names = stock_list.index.values.tolist()
    return names, ret.T.to_dict()

@exit_on_exception
@trace
def action(data_store,ticker_list, background) :
    ret = {}
    transpose = {}
    for ticker in ticker_list :
        prices = EXTRACT_TICKER.load(data_store, ticker)
        entry = TRANSFORM_BACKGROUND.find(prices)
        target = 'Security Name'
        entry['NAME'] = background[ticker].get(target,'')
        logging.debug(entry)
        ret[ticker] = entry
        for key in entry :
            if key not in transpose :
               transpose[key] = {}
            transpose[key][ticker] = entry[key]
    return ret, transpose

@exit_on_exception
@trace
def main() : 
    data_store = VARIABLES().data_store
    ticker_list, background = get_tickers()
    ret, transpose = action(data_store, ticker_list, background)

    INI_WRITE.write(VARIABLES().save_file,**transpose)
    logging.info("results saved to {}".format(VARIABLES().save_file))

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   save_file = '{}/local/stock_background.ini'.format(ENVIRONMENT.pwd_parent)
   data_store = '{}/local/historical_prices'.format(ENVIRONMENT.pwd_parent)

   main()

