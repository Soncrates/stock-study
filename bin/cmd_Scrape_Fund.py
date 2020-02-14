#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI, exit_on_exception, log_on_exception
from libDebug import trace, cpu
from libNASDAQ import NASDAQ, TRANSFORM_FUND as FUND
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
    @classmethod
    def config() :
        ini_list = EXTRACT.instance().config_list
        logging.info("loading results {}".format(ini_list))
        for path, section, key, stock_list in INI.loadList(*ini_list) :
            yield path, section, key, stock_list

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
      @classmethod
      def _summary(cls, data_store, ticker) :
          logging.info(ticker)
          prices = EXTRACT_TICKER.load(data_store, ticker)
          ret = TRANSFORM_TICKER.summary(prices)
          ret.rename(columns={0:ticker},inplace=True)
          return ret
      @classmethod
      def summary(cls, data_store, ticker_list) :
          ret = []
          for i, ticker in enumerate(ticker_list) :
              summary = cls._summary(data_store,ticker)
              ret.append(summary)
          ret = pd.concat(ret, axis=1)
          logging.info(ret)
          return ret.T.to_dict()
        
class LOAD() :

    @classmethod
    def config(cls, **config) :
        save_file = EXTRACT.instance().output_file_by_type
        ret = INI.init()
        for key in sorted(config) :
            value = config.get(key,[])
            INI.write_section(ret,key,**value)
        ret.write(open(save_file, 'w'))
        logging.info("results saved to {}".format(save_file))
    @classmethod
    def background(cls, **config) :
        save_file = EXTRACT.instance().background_file
        ret = INI.init()
        for key in sorted(config) :
            value = config.get(key,[])
            INI.write_section(ret,key,**value)
        ret.write(open(save_file, 'w'))
        logging.info("results saved to {}".format(save_file))

def process_names(fund_list) :
    ticker_list = map(lambda x : TRANSFORM.ticker(x), fund_list)
    ticker_list = list(ticker_list)
    name_list = map(lambda fund : TRANSFORM.name(fund), fund_list)
    name_list = map(lambda name : name.replace('%', ' percent'), name_list)
    name_list = map(lambda name : name.replace(' Fd', ' Fund'), name_list)
    ret = dict(zip(ticker_list,name_list))
    return ret

def process_by_type(fund_list) :
    recognized = FUND._Type.values()
    logging.info(fund_list[0])
    ret = {}
    for i, fund in enumerate(fund_list) :
        section = TRANSFORM.type(fund)
        key = TRANSFORM.category(fund)
        name = TRANSFORM.ticker(fund)
        if section is None or len(section) == 0 :
            continue
        if key is None or len(key) == 0 :
            continue
        if section not in recognized :
           key = "{}_{}".format(section,key)
           section = 'UNKNOWN'
        if section not in ret :
           ret[section] = {}
        curr = ret[section]
        if key not in curr :
           curr[key] = []
        curr[key].append(name)
    return ret

@exit_on_exception
@trace
def main() : 
    data_store = EXTRACT.instance().data_store

    fund_list = NASDAQ.init().fund_list()
    config = process_by_type(fund_list)
    LOAD.config(**config)

    names = process_names(fund_list)
    ticker_list = sorted(names.keys())
    background = TRANSFORM.summary(data_store, ticker_list)
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
   file_list = env.list_filenames('local/historical_prices/*pkl')
   output_file_by_type = '../local/fund_by_type.ini'
   background_file = '../local/fund_background.ini'

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '../local/historical_prices_fund'

   main()

