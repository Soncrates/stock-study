#!/usr/bin/env python

import logging as log
import pandas as PD
from libBusinessLogic import INI_READ,INI_WRITE
from libCommon import find_subset,LOG_FORMAT_TEST
from libNASDAQ import NASDAQ
from libBusinessLogic import YAHOO_SCRAPER, BASE_PANDAS_FINANCE as BPF
from libFinance import TRANSFORM_BACKGROUND
from libDecorators import singleton, exit_on_exception
from libDebug import trace, debug_object

@singleton
class GGG() :
    var_names = ['env','output_file',"data_store", 'sector','scraper']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*GGG.var_names))
        debug_object(self)

def get_tickers() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, alias = nasdaq.extract_stock_list()
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

def add_background(ticker, prices, background) :
    ret = TRANSFORM_BACKGROUND.find(prices)
    if ticker in background :
       ret.update(background[ticker])
    log.debug(ret)
    return ret

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
    ticker_list, background = get_tickers()
    background = enrich_background(GGG().sector, background)
    ret, transpose = action(GGG().data_store, ticker_list, background,GGG().scraper)
    INI_WRITE.write(GGG().output_file,**transpose)

if __name__ == '__main__' :
   import argparse
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)

   scraper = YAHOO_SCRAPER.pandas()
   output_file = '{}/outputs/stock_background.ini'.format(env.pwd_parent)
   data_store = '{}/local/historical_prices'.format(env.pwd_parent)
   sector = '{}/outputs/stock_by_sector.ini'.format(env.pwd_parent)

   parser = argparse.ArgumentParser(description='Process summary values for stocks')
   parser.add_argument('--data_store', action='store', dest='data_store', type=str, default=data_store, help='location of stock data')
   parser.add_argument('--output', action='store', dest='output_file', type=str, default=output_file, help='store report meta')
   parser.add_argument('--sector', action='store', dest='sector', type=str, default=sector, help='store report meta')
   cli = vars(parser.parse_args())

   main()