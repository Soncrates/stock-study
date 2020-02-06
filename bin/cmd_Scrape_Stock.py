#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI, exit_on_exception, log_on_exception
from libDebug import trace, cpu
from libNASDAQ import NASDAQ, NASDAQ_TRANSFORM
from libBackground import EXTRACT_TICKER, TRANSFORM_TICKER

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, config_list, file_list, output_file_by_type,background_file,local_dir,data_store) :
        self.env = _env
        self.config_list = config_list
        self.file_list = file_list
        self.output_file_by_type = output_file_by_type
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
        target = 'output_file_by_type'
        output_file_by_type = globals().get(target,None)
        if len(_env.argv) > 1 :
           output_file_by_type = _env.argv[1]
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
        cls._singleton = cls(_env,config_list,file_list, output_file_by_type,background_file,local_dir,data_store)
        return cls._singleton

class TRANSFORM() :
      _prices = 'Adj Close'
      @classmethod
      def _validate(cls, entry) :
          flag_1 = 'Symbol' in entry and 'Security Name' in entry
          flag_2 = 'NASDAQ Symbol' in entry and 'Security Name' in entry
          flag = flag_1 or flag_2
          return flag
      @classmethod
      def validate(cls, data) :
          data = filter(lambda x : cls._validate(x), data)
          return list(data)
      @classmethod
      def safe(cls, name) :
          name = name.replace('%', ' percent')
          return name
      @classmethod
      def symbol(cls, entry) :
          ret = entry.get('Symbol',None)
          if ret is None :
             ret = entry.get('NASDAQ Symbol',None)
          return ret
      @classmethod
      def to_dict(cls,stock_list, data) :
          ret = {}
          for i, ticker in enumerate(stock_list) :
              if '=' in ticker :
                  continue
              entry = filter(lambda x : cls.symbol(x) == ticker, data)
              entry = list(entry)
              if len(entry) == 0 :
                 continue
              entry = entry[0]
              ret[ticker] = cls.safe(entry['Security Name'])
          return ret
      @classmethod
      def summary(cls, data_store, ticker_list) :
          ret = []
          for ticker in ticker_list :
              logging.info(ticker)
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
        save_file = EXTRACT.instance().background_file
        ret = INI.init()
        for key in sorted(config) :
            value = config.get(key,[])
            INI.write_section(ret,key,**value)
        ret.write(open(save_file, 'w'))
        logging.info("results saved to {}".format(save_file))

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

@exit_on_exception
@trace
def main() : 
    data_store = EXTRACT.instance().data_store
    nasdaq = NASDAQ.init()

    stock_list, etf_list, alias = nasdaq.stock_list()
    stock_names, etf_names = process_names(nasdaq)
    names = {}
    names.update(etf_names)
    names.update(stock_names)
    ticker_list = sorted(names.keys())
    background = TRANSFORM.summary(data_store, stock_list)
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
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   #file_list = env.list_filenames('local/historical_prices/*pkl')
   output_file_by_type = '../local/stock_by_type.ini'
   background_file = '../local/stock_background.ini'

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '../local/historical_prices'

   main()

