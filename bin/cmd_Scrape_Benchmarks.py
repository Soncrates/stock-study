#!/usr/bin/env python

import logging as log
import pandas as pd
from libBusinessLogic import YAHOO_SCRAPER, TRANSFORM_TICKER, BASE_PANDAS_FINANCE
from libCommon import INI_WRITE, LOG_FORMAT_TEST
from libCommon import load_config, iterate_config, find_subset
from libUtils import mkdir
from libDecorators import singleton, exit_on_exception, log_on_exception
from libDebug import trace, debug_object
from libFinance import TRANSFORM_BACKGROUND

def _get_config(*config_list) :
    for i, path in enumerate(sorted(config_list)) :
        for j,k, section, key, value in iterate_config(load_config(path)):
            yield path, section, key, value

def get_benchmarks(config,benchmarks,omit_list) :
    ret = {}
    for path, section, key, stock in _get_config(config) :
        if section not in benchmarks :
           log.warning("Unknown section : {}".format(section))
           continue
        log.info((section,key,stock))
        ret[key] = stock
    for omit in omit_list :
        ret.pop(omit,None)
    log.info(ret)
    return ret

@singleton
class VARIABLES() :
    names = ['env','data_store','output_file','config_file','benchmarks','omit_list','scraper']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*VARIABLES.names))
        debug_object(self)
        
@exit_on_exception
@trace
def main() : 
    mkdir(VARIABLES().data_store)
    benchmark_list = get_benchmarks(VARIABLES().config_file, VARIABLES().benchmarks, VARIABLES().omit_list)
    ticker_list = benchmark_list.values()
   
    BASE_PANDAS_FINANCE.SAVE(VARIABLES().data_store, *ticker_list, **VARIABLES().scraper)

    ret = {}
    for ticker, data in BASE_PANDAS_FINANCE.LOAD(VARIABLES().data_store, *ticker_list) :
        ret[ticker] = TRANSFORM_BACKGROUND.find(data)
    ret = pd.DataFrame(ret)
    log.debug(ret)
        
    ret = TRANSFORM_TICKER.enhance(ret,benchmark_list)

    save_file = VARIABLES().output_file
    INI_WRITE.write(save_file,**ret)
    log.info("results saved to {}".format(save_file))

if __name__ == '__main__' :
   #import sys
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   data_store = '{}/local/historical_prices'.format(env.pwd_parent)
   output_file = "{}/local/benchmark_background.ini".format(env.pwd_parent)

   ini_list = env.list_filenames('local/*.ini')
   #config_file = filter(lambda x : 'benchmark' in x, ini_list)
   config_file = [ filename for filename in ini_list if 'benchmark' in filename ]
   benchmarks = ['Index','MOTLEYFOOL','PERSONAL']
   benchmarks = ['Index']
   omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']

   if len(env.argv) > 1 :
      config_file = [env.argv[1]]
   if len(env.argv) > 2 :
      output_file = [env.argv[2]]

   scraper = YAHOO_SCRAPER.pandas()

   main()
