#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI_READ,INI_WRITE, ENVIRONMENT, exit_on_exception, log_on_exception
from libFinance import STOCK_TIMESERIES 
from cmd_Scrape_Stock_Sector import DICT_HELPER
from libBackground import main as EXTRACT_BACKGROUND, load as TICKER, TRANSFORM_TICKER
from libDebug import trace

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, data_store, config_file,output_file) :
        self.env = _env
        self.data_store = data_store
        self.config_file = config_file
        self.output_file = output_file
        self.benchmarks = ['Index','MOTLEYFOOL','PERSONAL']
        self.benchmarks = ['Index']
        self.omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']
        msg = vars(self)
        for i, key in enumerate(sorted(msg)) :
            value = msg[key]
            if isinstance(value,list) and len(value) > 10 :
               value = value[:10]
            logging.info((i,key, value))
    @classmethod
    def instance(cls, **kwargs) :
        if not (cls._singleton is None) :
           return cls._singleton
        target = 'env'
        _env = globals().get(target,None)
        target = "data_store"
        data_store = globals().get(target,[])
        target = "output_file"
        output_file = globals().get(target,[])
        target = 'config_file'
        config_file = globals().get(target,[])
        if not isinstance(config_file,list) :
           config_file = list(config_file)
        if len(_env.argv) > 1 :
           config_file = [_env.argv[1]]
        if len(_env.argv) > 2 :
           output_file = [_env.argv[2]]
        env.mkdir(data_store)
        cls._singleton = cls(_env,data_store,config_file,output_file)
        return cls._singleton
    @classmethod
    def config(cls) :
        config = cls.instance().config_file
        logging.info("loading results {}".format(config))
        for path, section, key, stock_list in INI_READ.read(*config) :
            yield path, section, key, stock_list
    @classmethod
    def benchmarks(cls) :
        ret = DICT_HELPER.init()
        for path, section, key, stock in cls.config() :
            if section not in cls.instance().benchmarks :
                continue
            logging.info((section,key,stock))
            ret.append(key,*stock)
        for omit in cls.instance().omit_list :
            ret.data.pop(omit,None)
        logging.info(ret)
        stock_list = ret.values()
        return ret.data, stock_list

@exit_on_exception
@trace
def main() : 
    data_store = EXTRACT.instance().data_store

    data, stock_list = EXTRACT.benchmarks()
    TICKER(data_store=data_store, ticker_list=stock_list)
    ret = EXTRACT_BACKGROUND(data_store=data_store, ticker_list=stock_list)
    ret = ret.T
    names = TRANSFORM_TICKER.data(data)
    names = pd.DataFrame([names]).T
    ret['NAME'] = names

    save_file = EXTRACT.instance().output_file
    INI_WRITE.write(save_file,**ret)
    logging.info("results saved to {}".format(save_file))

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '{}/historical_prices'.format(local_dir)
   data_store = '../local/historical_prices'

   ini_list = env.list_filenames('local/*.ini')
   config_file = filter(lambda x : 'benchmark' in x, ini_list)
   output_file = "{}/benchmark_background.ini".format(local_dir)

   main()
