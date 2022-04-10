#!/usr/bin/env python

import logging as log
import pandas as pd
from libBusinessLogic import YAHOO_SCRAPER, TRANSFORM_TICKER, BASE_PANDAS_FINANCE
from libCommon import INI_WRITE, LOG_FORMAT_TEST
from libCommon import load_config, iterate_config, find_subset, find_files
from libUtils import mkdir
from libDecorators import singleton, exit_on_exception
from libFinance import TRANSFORM_BACKGROUND

def _get_config(*config_list) :
    for i, path in enumerate(sorted(config_list)) :
        for j,k, section, key, value in iterate_config(load_config(path)):
            yield path, section, key, value

def get_benchmarks(config_file_list,benchmarks) :
    ret = {}
    for path, section, key, stock in _get_config(*config_file_list) :
        if section not in benchmarks :
           log.warning("Unknown section : {}".format(section))
           continue
        log.info((section,key,stock))
        ret[key] = stock
    log.info(ret)
    return ret

def prep(config_file_list,benchmarks, omit_list) :
    ret = get_benchmarks(config_file_list , benchmarks )
    log.info(sorted(list(ret.keys())))
    ret = { key:value for (key,value) in ret.items() if key not in omit_list }
    log.info(sorted(list(ret.keys())))
    return ret

@singleton
class VARIABLES() :
    names = ['env','data_store','output_file','config_file_list','benchmarks','omit_list','scraper']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*VARIABLES.names))

@exit_on_exception
def main() : 
    mkdir(VARIABLES().data_store)
    
    benchmark_list = prep(VARIABLES().config_file_list , VARIABLES().benchmarks,VARIABLES().omit_list )
    ticker_list = list(benchmark_list.values())
    
    BASE_PANDAS_FINANCE.SAVE(VARIABLES().data_store, *ticker_list, **VARIABLES().scraper)
    #ret { ticker : TRANSFORM_BACKGROUND.find(data) for (ticker, data) in BASE_PANDAS_FINANCE.LOAD(VARIABLES().data_store, *ticker_list) }
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

   data_store  = '{pwd_parent}/local/historical_prices'.format(**vars(env))
   output_file = "{pwd_parent}/local/benchmark_background.ini".format(**vars(env))

   ini_list = '{pwd_parent}/local/*.ini'.format(**vars(env))
   ini_list = find_files(ini_list)
   config_file_list = [ filename for filename in ini_list if 'benchmark' in filename ]
   benchmarks = ['Index','MOTLEYFOOL','PERSONAL']
   benchmarks = ['Index']
   omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']

   if len(env.argv) > 1 :
      config_file = [env.argv[1]]
   if len(env.argv) > 2 :
      output_file = [env.argv[2]]

   scraper = YAHOO_SCRAPER.pandas()

   main()
