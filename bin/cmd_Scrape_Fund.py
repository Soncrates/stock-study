#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI_READ, INI_WRITE
from libNASDAQ import NASDAQ, TRANSFORM_FUND as FUND
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
def action(data_store,fund_list) : 
    ret = {}
    transpose = {}
    for ticker in fund_list.keys() :
        flag, _type, category, name = filter_by_type(fund_list[ticker])
        if flag :
           continue

        prices = EXTRACT_TICKER.load(data_store, ticker)
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
    ret, transpose = action(VARIABLES().data_store,fund_list)

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

   save_file = '{}/local/fund_background.ini'.format(env.pwd_parent)
   data_store = '{}/local/historical_prices_fund'.format(env.pwd_parent)

   main()

