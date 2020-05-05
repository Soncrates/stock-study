#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI_READ, INI_WRITE
from libUtils import ENVIRONMENT, DICT_HELPER, mkdir
from libBackground import main as EXTRACT_BACKGROUND, load as TICKER, TRANSFORM_TICKER
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

def _get_config(config) :
    logging.info("loading results {}".format(config))
    for path, section, key, stock_list in INI_READ.read(*config) :
        yield path, section, key, stock_list

def get_benchmarks(config,benchmarks,omit_list) :
    ret = DICT_HELPER.init()
    for path, section, key, stock in _get_config(config) :
        if section not in benchmarks :
           continue
        logging.info((section,key,stock))
        ret.append(key,*stock)
    for omit in omit_list :
        ret.data.pop(omit,None)
    logging.info(ret)
    return ret

@singleton
class VARIABLES() :
    var_names = ['env','data_store','output_file','config_file','benchmarks','omit_list']
    def __init__(self) :
        values = get_globals(*VARIABLES.var_names)
        self.__dict__.update(**values)
        debug_object(self)

        mkdir(self.data_store)
        if len(self.env.argv) > 1 :
           self.config_file = [self.env.argv[1]]
        if len(self.env.argv) > 2 :
           self.output_file = [self.env.argv[2]]

        data = get_benchmarks(self.config_file, self.benchmarks, self.omit_list)
        self.data = data.data
        self.stock_names = data.values()

@exit_on_exception
@trace
def main() : 
    data_store = VARIABLES().data_store

    data = VARIABLES().data
    stock_list = VARIABLES().stock_names
    TICKER(data_store=data_store, ticker_list=stock_list)
    ret = EXTRACT_BACKGROUND(data_store=data_store, ticker_list=stock_list)
    ret = ret.T
    names = TRANSFORM_TICKER.data(data)
    names = pd.DataFrame([names]).T
    ret['NAME'] = names

    save_file = VARIABLES().output_file
    INI_WRITE.write(save_file,**ret)
    logging.info("results saved to {}".format(save_file))

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   data_store = '{}/local/historical_prices'.format(env.pwd_parent)
   output_file = "{}/local/benchmark_background.ini".format(env.pwd_parent)

   ini_list = env.list_filenames('local/*.ini')
   config_file = filter(lambda x : 'benchmark' in x, ini_list)
   benchmarks = ['Index','MOTLEYFOOL','PERSONAL']
   benchmarks = ['Index']
   omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']

   main()
