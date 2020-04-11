#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI_BASE, INI_WRITE, exit_on_exception, log_on_exception
from libDebug import trace, cpu
from libNASDAQ import NASDAQ, NASDAQ_TRANSFORM
#from libBackground import main as EXTRACT_BACKGROUND
from libBackground import EXTRACT_TICKER
from libFinance import TRANSFORM_BACKGROUND


class EXTRACT() :
    _singleton = None
    def __init__(self, _env, config_list, file_list, background_file,local_dir,data_store) :
        self.env = _env
        self.config_list = config_list
        self.file_list = file_list
        #self.dep_output_file_by_type = output_file_by_type
        self.background_file = background_file
        self.local_dir = local_dir
        self.data_store = data_store
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
        target = 'background_file'
        background_file = globals().get(target,None)
        #target = 'output_file_by_type'
        #output_file_by_type = globals().get(target,None)
        #if len(_env.argv) > 1 :
        #   output_file_by_type = _env.argv[1]
        target = 'ini_list'
        config_list = globals().get(target,[])
        if not isinstance(config_list,list) :
           config_list = list(config_list)
        target = "file_list"
        file_list = globals().get(target,[])
        target = "local_dir"
        local_dir = globals().get(target,None)
        target = "data_store"
        data_store = globals().get(target,[])
        cls._singleton = cls(_env,config_list,file_list, background_file,local_dir,data_store)
        return cls._singleton

class TRANSFORM() :
      _prices = 'Adj Close'
      _security = 'Security Name'
      _primary = 'Symbol'
      _secondary = 'NASDAQ Symbol'
      @classmethod
      def _validate(cls, entry) :
          flag_1 = cls._primary in entry and cls._security in entry
          flag_2 = cls._secondary in entry and cls._security in entry
          flag = flag_1 or flag_2
          return flag
      @classmethod
      def validate(cls, data) :
          ret = filter(lambda x : cls._validate(x), data)
          return list(ret)
      @classmethod
      def safe(cls, name) :
          name = name.replace('%', ' percent')
          return name
      @classmethod
      def get_symbol(cls, entry) :
          ret = entry.get(cls._primary,None)
          if not (ret is None) :
             return ret
          return entry.get(cls._secondary,None)
      @classmethod
      def get_name(cls,stock_list, data) :
          for i, ticker in enumerate(stock_list) :
              if '=' in ticker :
                  continue
              ret = filter(lambda x : cls.get_symbol(x) == ticker, data)
              ret = list(ret)
              if len(ret) == 0 :
                 continue
              yield i, ticker, ret[0]
      @classmethod
      def to_dict(cls,stock_list, data) :
          ret = {}
          for i, ticker, entry in cls.get_name(stock_list, data) :
              value = entry[cls._security]
              ret[ticker] = cls.safe(value)
          return ret

def process_names(nasdaq) :
    listed, csv = nasdaq.listed()
    listed = list(listed)
    other, csv = nasdaq.other()
    other = list(other)
    data = []
    data += listed + other
    data = TRANSFORM.validate(data)

    stock, etf, alias = NASDAQ_TRANSFORM.stock_list(data)
    stock_list = sorted(stock)
    stock_names = TRANSFORM.to_dict(stock_list,data)
    etf_list = sorted(etf)
    etf_names = TRANSFORM.to_dict(etf_list,data)
    return stock_names, etf_names

def init() :
    nasdaq = NASDAQ.init()
    stock_list, etf_list, alias = nasdaq.stock_list()
    stock_names, etf_names = process_names(nasdaq)
    names = {}
    names.update(etf_names)
    names.update(stock_names)
    return names, stock_list, etf_list, alias, stock_names, etf_names 

@exit_on_exception
@trace
def action(data_store,ticker_list) :
    ret = {}
    transpose = {}
    for ticker in ticker_list :
        prices = EXTRACT_TICKER.load(data_store, ticker)
        entry = TRANSFORM_BACKGROUND.find(prices)
        entry['NAME'] = ticker_list[ticker]
        logging.debug(entry)
        ret[ticker] = entry
        for key in entry :
            if key not in transpose :
               transpose[key] = {}
            transpose[key][ticker] = entry[key]
    return ret, transpose

@exit_on_exception
@trace
def main() : 
    data_store = EXTRACT.instance().data_store
    names, stock_list, etf_list, alias, stock_names, etf_names = init()
    ret, transpose = action(data_store, names)

    save_file = EXTRACT.instance().background_file
    INI_WRITE.write(save_file,**transpose)
    logging.info("results saved to {}".format(save_file))

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   #file_list = env.list_filenames('local/historical_prices/*pkl')
   #output_file_by_type = '../local/stock_by_type.ini'
   background_file = '../local/stock_background.ini'

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '../local/historical_prices'

   main()

