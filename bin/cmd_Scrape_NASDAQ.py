#!/usr/bin/env python
import sys
import logging

if sys.version_info < (3, 0):
   import pandas as pd
   import pandas_datareader as web
   from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
else :
   '''
       import pandas_datareader as web
         ModuleNotFoundError: No module named 'pandas_datareader'
   '''
   import pandas as pd
   import pandas_datareader as web
   from pandas_datareader.nasdaq_trader import get_nasdaq_symbols

from libCommon import log_exception
from libDebug import trace

'''
  NASDAQ - wrapper class around pandas built-in nasdaq reader
         , creates csv of all nasdaq stocks and funds

'''
class HELPER :
      @classmethod
      def transform(cls, stock) :
          if '-' not in stock :
             return stock
          stock = stock.split('-')
          stock = '-P'.join(stock)
          return stock
      @classmethod
      def extract(cls, results) :
          symbol_list = filter(lambda x : 'Symbol' in x, results.columns)
          if not isinstance(symbol_list,list) :
             symbol_list = list(symbol_list)
          for index, row in results.iterrows():
              symbol_value = map(lambda x : row[x],symbol_list)
              ret = dict(zip(symbol_list,symbol_value))
              logging.info(ret)
              yield symbol_list[1], symbol_list[0], ret

class NASDAQ :
      path = 'nasdaq.csv'
      def __init__(self, results) :
          self.results = results
      def __call__(self) :
          if self.results is None : 
             return
          for name, alt_name, row in HELPER.extract(self.results) :
              stock = row.get(name)
              if not isinstance(stock,str) :
                 stock = row.get(alt_name)
              stock = HELPER.transform(stock)
              yield stock
      @classmethod
      def init(cls, **kwargs) :
          target = 'filename'
          filename = kwargs.get(target, None)
          target = 'retry_count'
          retry_count = kwargs.get(target,3)
          target = 'timeout'
          timeout = kwargs.get(target,30)

          results = get_nasdaq_symbols(retry_count, timeout)
          if filename is not None :
             results.to_csv(filename)
          ret = cls(results)
          return ret

@log_exception
@trace
def main(save_file) :
    nasdaq = NASDAQ.init(filename=save_file)
    for stock in nasdaq() :
        logging.info(stock)
        if stock == 'AAPL' : break

if __name__ == '__main__' :
   import logging
   import sys
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   #nasdaq = env.list_filenames('local/'+NASDAQ.path)
   save_file = '{}/local/{}'.format(env.pwd_parent,NASDAQ.path)
   main(save_file)
