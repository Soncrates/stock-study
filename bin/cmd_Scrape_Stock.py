#!/usr/bin/env python

import logging as log
import pandas as PD
from libCommon import INI_WRITE, INI_READ, find_subset,LOG_FORMAT_TEST
from libNASDAQ import NASDAQ
from libBusinessLogic import YAHOO_SCRAPER, ROBUST_PANDAS_FINANCE as RPF, BASE_PANDAS_FINANCE as BPF
from libFinance import TRANSFORM_BACKGROUND
from libDecorators import singleton, exit_on_exception, log_on_exception
from libDebug import trace, debug_object

@singleton
class VARIABLES() :
    var_names = ['env','save_file',"data_store", 'sector_file','scraper']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*VARIABLES.var_names))
        debug_object(self)

def get_tickers() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, alias = nasdaq.stock_list()
    ret = PD.concat([stock_list, etf_list])
    names = stock_list.index.values.tolist()
    return names, ret.T.to_dict()

def enrich_background(sector_file,background, entity = 'stock') :
    ret = {}
    for section, key, ticker_list in INI_READ.read(*[sector_file]) :
        for ticker in ticker_list :
            name = background.get(ticker,{})
            name = name.get('Security Name','')
            ret[ticker] = { 'SECTOR' : key, 'NAME' : name, 'ENTITY' : entity }
    return ret

def dep_add_background(ticker, filename, background,scraper) :
    prices = RPF.SAFE(filename,scraper)
    ret = TRANSFORM_BACKGROUND.find(prices)
    if ticker in background :
       ret.update(background[ticker])
    log.debug(ret)
    return ret
def add_background(ticker, prices, background) :
    ret = TRANSFORM_BACKGROUND.find(prices)
    if ticker in background :
       ret.update(background[ticker])
    log.debug(ret)
    return ret

@exit_on_exception
@trace
def dep_action(data_store,ticker_list, background,scraper) :
    ret = {}
    transpose = {}
    for i, args in BPF.make_args(*ticker_list, **scraper) :
        filename ="{}/{}.pkl".format(data_store,args['ticker'])
        entry = add_background(args['ticker'],filename,background,args)
        ret[args['ticker']] = entry
        for key in entry :
            if key not in transpose :
               transpose[key] = {}
            transpose[key][args['ticker']] = entry[key]
        del args
    return ret, transpose

@exit_on_exception
@trace
def action(data_store,ticker_list, background,scraper) :
    ret = {}
    transpose = {}
    for ticker, data in BPF.LOAD(data_store, *ticker_list) :
        entry = add_background(ticker,data,background)
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
    ret, transpose = action(data_store, ticker_list, background,VARIABLES().scraper)

    INI_WRITE.write(VARIABLES().save_file,**transpose)

if __name__ == '__main__' :
   import sys
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   scraper = YAHOO_SCRAPER.pandas()
   save_file = '{}/local/stock_background.ini'.format(env.pwd_parent)
   data_store = '{}/local/historical_prices'.format(env.pwd_parent)
   sector_file = '{}/local/stock_by_sector.ini'.format(env.pwd_parent)

   main()