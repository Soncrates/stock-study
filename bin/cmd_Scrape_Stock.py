#!/usr/bin/env python

import logging

import pandas as pd
from libCommon import INI_BASE, INI_WRITE, INI_READ
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
    var_names = ['env','save_file',"data_store", 'sector_file']
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

def enrich_background(sector_file,background, entity = 'stock') :
    logging.info('reading file {}'.format(sector_file))
    ret = {}
    for path, section, key, ticker_list in INI_READ.read(*[sector_file]) :
        for ticker in ticker_list :
            name = background.get(ticker,{})
            name = name.get('Security Name','')
            ret[ticker] = { 'SECTOR' : key, 'NAME' : name, 'ENTITY' : entity }
    return ret

def add_background(ticker,data_store, background) :
    prices = EXTRACT_TICKER.load(data_store, ticker)
    ret = TRANSFORM_BACKGROUND.find(prices)
    if ticker in background :
       ret.update(background[ticker])
    logging.debug(ret)
    return ret

@exit_on_exception
@trace
def action(data_store,ticker_list, background) :
    ret = {}
    transpose = {}
    for ticker in ticker_list :
        entry = add_background(ticker,data_store,background)
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
    background = enrich_background(VARIABLES().sector_file, background)
    ret, transpose = action(data_store, ticker_list, background)

    INI_WRITE.write(VARIABLES().save_file,**transpose)
    logging.info("results saved to {}".format(VARIABLES().save_file))

if __name__ == '__main__' :
   import sys
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   save_file = '{}/local/stock_background.ini'.format(env.pwd_parent)
   data_store = '{}/local/historical_prices'.format(env.pwd_parent)
   sector_file = '{}/local/stock_by_sector.ini'.format(env.pwd_parent)

   main()

