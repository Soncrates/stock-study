#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI_READ, INI_WRITE
from libUtils import exit_on_exception, log_on_exception
from libDebug import trace, cpu
from libNASDAQ import NASDAQ, TRANSFORM_FUND as FUND
from libBackground import EXTRACT_TICKER
from libFinance import TRANSFORM_BACKGROUND

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
        for path, section, key, stock_list in INI_READ.read(*ini_list) :
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
      def dep_safe(cls, name) :
          name = name.replace('%', ' percent')
          return name
      @classmethod
      def dep_get_symbol(cls, entry) :
          ret = entry.get(cls._primary,None)
          if not (ret is None) :
             return ret
          return entry.get(cls._secondary,None)
      @classmethod
      def dep_get_name(cls,stock_list, data) :
          for i, ticker in enumerate(stock_list) :
              if '=' in ticker :
                  continue
              ret = filter(lambda x : cls.get_symbol(x) == ticker, data)
              ret = list(ret)
              if len(ret) == 0 :
                 continue
              yield i, ticker, ret[0]
      @classmethod
      def dep_to_dict(cls,stock_list, data) :
          ret = {}
          for i, ticker, entry in cls.get_name(stock_list, data) :
              value = entry[cls._security]
              ret[ticker] = cls.safe(value)
          return ret
        
class LOAD() :

    @classmethod
    def config(cls, **config) :
        save_file = EXTRACT.instance().output_file_by_type
        INI_WRITE.write(save_file,**config)
        logging.info("results saved to {}".format(save_file))

def filter_by_type(fund) :
    target = 'Type'
    section = fund.get(target, None)
    target = 'Category'
    category = fund.get(target, None)

    if section is None or len(section) == 0 :
       return True, None, None, None, None
    if category is None or len(category) == 0 :
       return True, None, None, None, None
    target = 'Fund Symbol'
    ticker = fund.get(target,None)
    target = 'Fund Name'
    name = fund.get(target,None)
    name = name.replace('%', ' percent')
    name = name.replace(' Fd', ' Fund')
    recognized = FUND.TYPE.values()
    if section not in recognized :
       category = "{}_{}".format(section,category)
       section = 'UNKNOWN'
    return False, section, category, name, ticker

@exit_on_exception
@trace
def action(data_store,fund_list) : 
    ret = {}
    transpose = {}
    for fund in fund_list :
        flag, section, category, name, ticker = filter_by_type(fund)
        if flag :
           continue

        prices = EXTRACT_TICKER.load(data_store, ticker)
        entry = TRANSFORM_BACKGROUND.find(prices)
        del prices
        if 'LEN' not in entry :
           continue
        entry['NAME'] = name
        entry['CATEGORY'] = category
        entry['TYPE'] = section
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
    fund_list = NASDAQ.init().fund_list()
    ret, transpose = action(data_store,fund_list)

    save_file = EXTRACT.instance().background_file
    INI_WRITE.write(save_file,**transpose)
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

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   output_file_by_type = '../local/fund_by_type.ini'
   background_file = '../local/fund_background.ini'

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '../local/historical_prices_fund'

   main()

