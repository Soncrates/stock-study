#!/usr/bin/env python

import logging
import sys, os
from libCommon import INI, log_exception, ENVIRONMENT
from libFinance import STOCK_TIMESERIES
from cmd_Scrape_BackGround import DICT_HELPER
from libDebug import trace

def prep() :
    target = 'data_store'
    data_store = globals().get(target,'')
    if not isinstance(data_store,str) :
       data_store = str(data_store)
    logging.info('making data store {}'.format(data_store))
    ENVIRONMENT.mkdir(data_store)
    target = 'ini_list'
    ini_list = globals().get(target,[])
    if not isinstance(ini_list,list) :
       ini_list = list(ini_list)
    logging.info('loading config files {}'.format(ini_list))
    ret = DICT_HELPER.init()
    target = ['Index','MOTLEYFOOL']
    for path, section, key, stock in INI.loadList(*ini_list) :
        if section not in target :
            continue
        ret.append(key,*stock)
    omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']
    for omit in omit_list :
        ret.data.pop(omit,None)
    logging.info(ret)
    stock_list = ret.values()
    return ret.data, stock_list

@log_exception
@trace
def main(local_dir ) : 
    data, stock_list = prep()

    reader = STOCK_TIMESERIES.init()
    dud = []
    for stock in stock_list :
        filename = '{}/historical_prices/{}.pkl'.format(local_dir,stock)
        ret = reader.extract_from_yahoo(stock)
        if ret is None :
           dud.append(stock)
           continue
        logging.debug(ret.tail(5))
        STOCK_TIMESERIES.save(filename, stock, ret)
    size = len(stock_list) - len(dud)
    logging.info("Total {}".format(size))
    if len(dud) > 0 :
       logging.warn(dud)

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '{}/historical_prices'.format(local_dir)

   ini_list = env.list_filenames('local/*.ini')
   ini_list = filter(lambda x : 'benchmark' in x, ini_list)

   main(local_dir)

