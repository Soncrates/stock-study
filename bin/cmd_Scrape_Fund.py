#!/usr/bin/env python

import logging

from libBusinessLogic import YAHOO_SCRAPER, ROBUST_PANDAS_FINANCE as PANDAS_FINANCE, BASE_PANDAS_FINANCE as BPF
from libCommon import INI_WRITE,find_subset,LOG_FORMAT_TEST
from libNASDAQ import NASDAQ, TRANSFORM_FUND as FUND
from libFinance import TRANSFORM_BACKGROUND
from libDecorators import singleton, exit_on_exception, log_on_exception
from libDebug import trace, debug_object

@singleton
class VARIABLES() :
    var_names = ['env','save_file',"data_store",'scraper']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*VARIABLES.var_names))
        if len(self.env.argv) > 1 :
           self.output_file_by_type = self.env.argv[1]
        debug_object(self)

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
@trace
def dep_action(data_store,fund_list,scraper) : 
    ret = {}
    transpose = {}
    for i, args in BPF.make_args(*fund_list.keys(), **scraper) :
        ticker = args['ticker']
        flag, _type, category, name = filter_by_type(fund_list[ticker])
        if flag :
           del args
           continue
        filename ="{}/{}.pkl".format(data_store,ticker)
        scraper['ticker'] = ticker
        prices = PANDAS_FINANCE.SAFE(filename,args)
        entry = TRANSFORM_BACKGROUND.find(prices)
        del prices
        del args
        if 'LEN' not in entry :
           continue
        entry['NAME'] = name
        entry['CATEGORY'] = category
        entry['TYPE'] = _type
        logging.debug(entry)
        ret[ticker] = entry
        for key in entry :
            if key not in transpose :
               transpose[key] = {}
            transpose[key][ticker] = entry[key]
    return ret, transpose

@exit_on_exception
@trace
def action(data_store,fund_list,scraper) : 
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
        logging.debug(entry)
        ret[ticker] = entry
        for key in entry :
            if key not in transpose :
               transpose[key] = {}
            transpose[key][ticker] = entry[key]
    return ret, transpose

def get_tickers() : 
    ret = NASDAQ.init().fund_list()
    logging.info(ret)
    ret = ret.T.to_dict()
    return ret

@exit_on_exception
@trace
def main() : 
    fund_list = get_tickers()
    ret, transpose = action(VARIABLES().data_store,fund_list,VARIABLES().scraper)

    INI_WRITE.write(VARIABLES().save_file,**transpose)
    logging.info("results saved to {}".format(VARIABLES().save_file))

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   logging.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   scraper = YAHOO_SCRAPER.pandas()
   save_file = '{}/local/fund_background.ini'.format(env.pwd_parent)
   data_store = '{}/local/historical_prices_fund'.format(env.pwd_parent)

   main()

