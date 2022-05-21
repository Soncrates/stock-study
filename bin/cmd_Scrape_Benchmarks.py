#!/usr/bin/env python

import logging as log
import pandas as pd
from libBusinessLogic import INI_WRITE
from libBusinessLogic import YAHOO_SCRAPER, TRANSFORM_TICKER, BASE_PANDAS_FINANCE
from libCommon import LOG_FORMAT_TEST
from libCommon import load_config, iterate_config, find_files
from libUtils import mkdir
from libDecorators import exit_on_exception
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

@exit_on_exception
def main(**args) : 
    mkdir(args.get('data_store'))
    
    benchmark_list = prep( [ args['config_file'] ] , args['benchmarks'],args['omit_list'] )
    ticker_list = list(benchmark_list.values())
    
    BASE_PANDAS_FINANCE.SAVE(args['data_store'], *ticker_list, **args['scraper'])
    ret = {}
    for ticker, data in BASE_PANDAS_FINANCE.LOAD(args['data_store'], *ticker_list) :
        ret[ticker] = TRANSFORM_BACKGROUND.find(data)
    ret = pd.DataFrame(ret)
    log.debug(ret)
        
    ret = TRANSFORM_TICKER.enhance(ret,benchmark_list)
    INI_WRITE.write(args['output_file'],**ret)

if __name__ == '__main__' :
   import argparse
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)

   data_store  = '{pwd_parent}/local/historical_prices'.format(**vars(env))
   output_file = "{pwd_parent}/outputs/benchmark_background.ini".format(**vars(env))

   ini_list = '{pwd_parent}/local/*.ini'.format(**vars(env))
   ini_list = find_files(ini_list)
   config_file_list = [ filename for filename in ini_list if 'benchmark' in filename ]
   config_file = config_file_list[0]
   benchmarks = ['Index','MOTLEYFOOL','PERSONAL']
   benchmarks = ['Index']
   omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']

   parser = argparse.ArgumentParser(description='Scrape Benchmarks')
   parser.add_argument('--input', action='store', dest='data_store', type=str, default=data_store, help='dirctory to write in')
   parser.add_argument('--output', action='store', dest='output_file', type=str, default=output_file, help='store report meta')
   parser.add_argument('--config', action='store', dest='config_file', type=str, default=config_file, help='read in data')

   scraper = YAHOO_SCRAPER.pandas()

   names = ['env','data_store','output_file','config_file_list','config_file','benchmarks','omit_list','scraper']
   init = { key : value for (key,value) in globals().items() if key in names }
   main(**init)
