#!/usr/bin/env python

import logging as log

from libBusinessLogic import YAHOO_SCRAPER, BASE_PANDAS_FINANCE as BPF
from libBusinessLogic import INI_WRITE
from libCommon import find_subset,LOG_FORMAT_TEST
from libNASDAQ import NASDAQ, TRANSFORM_FUND as FUND
from libFinance import TRANSFORM_BACKGROUND
from libDecorators import singleton, exit_on_exception
from libDebug import trace

@singleton
class GGG() :
    var_names = ['env','output_file',"data_store",'scraper']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*GGG.var_names))

def filter_by_type(fund) :
    target = 'Type'
    _type = fund.get(target, None)
    target = 'Category'
    category = fund.get(target, None)

    if _type is None or len(_type) == 0 :
       return True, None, None, None
    if category is None or len(category) == 0 :
       return True, None, None, None
    target = 'Fund Name'
    name = fund.get(target,None)
    name = name.replace('%', ' percent')
    name = name.replace(' Fd', ' Fund')
    recognized = FUND.TYPE.values()
    if _type not in recognized :
       category = "{}_{}".format(_type,category)
       _type = 'UNKNOWN'
    return False, _type, category, name

@exit_on_exception
def transform(data_store,fund_list,scraper) : 
    ret = {}
    transpose = {}
    for ticker, prices in BPF.LOAD(data_store, *fund_list) :
        flag, _type, category, name = filter_by_type(fund_list[ticker])
        if flag :
           continue
        entry = TRANSFORM_BACKGROUND.find(prices)
        del prices
        if 'LEN' not in entry :
           continue
        entry['NAME'] = name
        entry['CATEGORY'] = category
        entry['TYPE'] = _type
        log.debug(entry)
        ret[ticker] = entry
        for key in entry :
            if key not in transpose :
               transpose[key] = {}
            transpose[key][ticker] = entry[key]
    return ret, transpose

@exit_on_exception
@trace
def main() : 
    fund_list, csv = NASDAQ.init().extract_fund_list()
    fund_list = fund_list.T.to_dict()
    ret, transpose = transform(GGG().data_store,fund_list,GGG().scraper)
    INI_WRITE.write(GGG().output_file,**transpose)

if __name__ == '__main__' :
   import argparse
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)

   scraper = YAHOO_SCRAPER.pandas()
   output_file = '{}/outputs/fund_background.ini'.format(env.pwd_parent)
   data_store = '{}/local/historical_prices_fund'.format(env.pwd_parent)

   parser = argparse.ArgumentParser(description='Scrape Funds')
   parser.add_argument('--input', action='store', dest='data_store', type=str, default=data_store, help='portfolios to read in')
   parser.add_argument('--output', action='store', dest='output_file', type=str, default=output_file, help='store report meta')
   cli = vars(parser.parse_args())
   
   main()

