#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI, ENVIRONMENT, exit_on_exception, log_on_exception
from libFinance import STOCK_TIMESERIES
from cmd_Scrape_Stock_Sector import DICT_HELPER
from libBackground import EXTRACT_TICKER, TRANSFORM_TICKER
from libDebug import trace

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, data_store, config_file,output_file, reader) :
        self.env = _env
        self.data_store = data_store
        self.config_file = config_file
        self.output_file = output_file
        self.reader = reader
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
        reader = STOCK_TIMESERIES.init()
        env.mkdir(data_store)
        cls._singleton = cls(_env,data_store,config_file,output_file,reader)
        return cls._singleton
    @classmethod
    def config(cls) :
        config = cls.instance().config_file
        logging.info("loading results {}".format(config))
        for path, section, key, stock_list in INI.loadList(*config) :
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
    @classmethod
    @trace
    def robust(cls,data_store, ticker_list) :
        retry = EXTRACT_TICKER.save_list(data_store, ticker_list)
        if len(retry) > 0 :
           retry = EXTRACT_TICKER.save_list(data_store, retry)
        if len(retry) > 0 :
           logging.error((len(retry), sorted(retry)))

class TRANSFORM() :
    def summary(data_store, ticker_list) :
        ret = []
        for ticker in ticker_list :
            prices = EXTRACT_TICKER.load(data_store, ticker)
            summary = TRANSFORM_TICKER.summary(prices)
            summary.rename(columns={0:ticker},inplace=True)
            ret.append(summary)
        ret = pd.concat(ret, axis=1)
        logging.info(ret)
        return ret.T.to_dict()

class LOAD() :
      @classmethod
      def background(cls, **config) :
          save_file = EXTRACT.instance().output_file
          ret = INI.init()
          for key in sorted(config) :
              value = config.get(key,[])
              INI.write_section(ret,key,**value)
          ret.write(open(save_file, 'w'))
          logging.info("results saved to {}".format(save_file))

@exit_on_exception
@trace
def main() : 
    data_store = EXTRACT.instance().data_store

    data, stock_list = EXTRACT.benchmarks()
    EXTRACT.robust(data_store, stock_list)
    background = TRANSFORM.summary(data_store, stock_list)
    names = TRANSFORM_TICKER.data(data)
    background['NAME'] = names
    LOAD.background(**background)

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
   data_store = '../local/historical_prices'

   ini_list = env.list_filenames('local/*.ini')
   config_file = filter(lambda x : 'benchmark' in x, ini_list)
   output_file = "{}/benchmark_background.ini".format(local_dir)

   main()
