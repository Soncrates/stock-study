#!/usr/bin/env python

import logging
from libCommon import INI, exit_on_exception, ENVIRONMENT
from libFinance import STOCK_TIMESERIES
from cmd_Scrape_Stock_Sector import DICT_HELPER
from libDebug import trace

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, data_store, config_file, reader) :
        self.env = _env
        self.data_store = data_store
        self.config_file = config_file
        self.reader = reader
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
        target = 'config_file'
        config_file = globals().get(target,[])
        if not isinstance(config_file,list) :
           config_file = list(config_file)
        if len(_env.argv) > 1 :
           config_file = [_env.argv[1]]
        reader = STOCK_TIMESERIES.init()
        env.mkdir(data_store)
        cls._singleton = cls(_env,data_store,config_file,reader)
        return cls._singleton
    @classmethod
    def config(cls) :
        config = cls.instance().config_file
        logging.info("loading results {}".format(config))
        for path, section, key, stock_list in INI.loadList(*config) :
            yield path, section, key, stock_list

class LOAD() :
      @classmethod
      def _prices(cls, local_dir, ticker,dud) :
          filename = '{}/{}.pkl'.format(local_dir,ticker)
          reader =  EXTRACT.instance().reader
          prices = reader.extract_from_yahoo(ticker)
          if prices is None :
             dud.append(ticker)
             return dud
          STOCK_TIMESERIES.save(filename, ticker, prices)
          return dud
      @classmethod
      def prices(cls,local_dir, ticker_list) :
          dud = []
          for ticker in ticker_list :
              dud = cls._prices(local_dir, ticker,dud)
          size = len(ticker_list) - len(dud)
          logging.info("Total {}".format(size))
          if len(dud) > 0 :
             dud = sorted(dud)
             logging.warn((len(dud),dud))
          return dud
      @classmethod
      @trace
      def robust(cls,data_store, ticker_list) :
          retry = cls.prices(data_store, ticker_list)
          if len(retry) > 0 :
             retry = cls.prices(data_store, retry)
          if len(retry) > 0 :
             logging.error((len(retry), sorted(retry)))

def extract() :
    ret = DICT_HELPER.init()
    target = ['Index','MOTLEYFOOL','PERSONAL']
    for path, section, key, stock in EXTRACT.config() :
        if section not in target :
            continue
        ret.append(key,*stock)
    omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']
    for omit in omit_list :
        ret.data.pop(omit,None)
    logging.info(ret)
    stock_list = ret.values()
    return ret.data, stock_list

@exit_on_exception
@trace
def main() : 
    data, stock_list = extract()
    data_store = EXTRACT.instance().data_store
    LOAD.robust(data_store, stock_list)

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

   main()

