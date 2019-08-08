from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
import pandas_datareader as web
import pandas as pd
import datetime
import ConfigParser

from itertools import combinations as iter_combo

def combinations(stock_list,size=5) :
    ret_list = iter_combo(stock_list,size)
    for ret in list(ret_list):
        yield list(ret)

class NASDAQ :
      path = 'nasdaq.csv'
      @staticmethod
      def init(**kwargs) :
          target = 'filename'
          filename = kwargs.get(target, None)
          target = 'retry_count'
          retry_count = kwargs.get(target,3)
          target = 'timeout'
          timeout = kwargs.get(target,30)

          results = get_nasdaq_symbols(retry_count, timeout)
          if filename is not None :
             results.to_csv(filename)
          ret = NASDAQ(results)
          return ret

      def __init__(self, results) :
          self.results = results

      def __call__(self) :
          if self.results is None : return
          for name, alt_name, row in self._extract() :
              stock = row.get(name)
              if not isinstance(stock,str) :
                 stock = row.get(alt_name)
              if '-' in stock :
                 stock = stock.split('-')
                 stock = '-P'.join(stock)
              yield stock

      def _extract(self) :
          results = self.results
          symbol_list = filter(lambda x : 'Symbol' in x, results.columns)
          for index, row in results.iterrows():
              symbol_value = map(lambda x : row[x],symbol_list)
              ret = dict(zip(symbol_list,symbol_value))
              yield symbol_list[1], symbol_list[0], ret

class INI(object) :
      @staticmethod
      def init() :
          ret = ConfigParser.ConfigParser()
          ret.optionxform=str
          return ret
      @staticmethod
      def _init(ini_file) :
          config = INI.init()
          ret = open(ini_file)
          config.readfp(ret)
          return config

      @staticmethod
      def loadList(*file_list) :
          file_list = sorted(file_list)
          for ini_file in file_list :
              for name, key, value in INI._loadList(ini_file) :
                  yield ini_file, name, key, value

      @staticmethod
      def _loadList(path) :
          if path.endswith('ini') == False : return
          config = INI._init(path)
          for name, key, value in INI.read_section(config) :
              if ',' in value :
                 value = value.split(',')
                 value = map(lambda key : key.strip(), value)
              else :
                     value = [value]
              yield name, key, value

      @staticmethod
      def read_section(config) :
          for name in sorted(config._sections) :
              for key, value in config.items(name) :
                  if len(value) == 0 : continue
                  yield name, key, value

      @staticmethod
      def write_section(config,section,**data) :
          config.add_section(section)
          for key in sorted(data.keys()) :
              value = data[key]
              if isinstance(value,list) :
                 value = ",".join(value)
              config.set(section,key,value)

class STOCK_TIMESERIES :
      @staticmethod
      def init(**kwargs) :
          target = 'end'
          end = kwargs.get(target, datetime.datetime.utcnow())
          target = 'start'
          start = kwargs.get(target, datetime.timedelta(days=365*10))
          start = end - start
          ret = STOCK_TIMESERIES(start,end)
          return ret
      def __init__(self,start,end) :
          self.start = start
          self.end   = end
      def extract_from_yahoo(self, stock) :
          ret = self._extract_from(stock, 'yahoo') 
          return ret
      def _extract_from(self, stock, service) :
          try :
             return web.DataReader(stock, service, self.start, self.end) 
          except Exception, e:
             print e

      @staticmethod
      def save(filename, stock, data) :
          if data is None : return
          data['Stock'] = stock
          data.to_pickle(filename)

      @staticmethod
      def load(filename) :
          data = pd.read_pickle(filename)
          target = 'Stock'
          if target in data :
             name = data.pop(target)
             name = name[0]
             return name, data
          name = filename.split("/")[-1]
          name = name.split(".")[0]
          return name, data
      @staticmethod
      def read(file_list, stock_list) :
          if stock_list is None or len(stock_list) == 0 :
             for path in file_list :
                 name, ret = STOCK_TIMESERIES.load(path)
                 yield name, ret
             return
              
          for path in file_list :
              flag_maybe = filter(lambda x : x in path, stock_list)
              flag_maybe = len(flag_maybe) > 0
              if not flag_maybe : continue
              name, ret = STOCK_TIMESERIES.load(path)
              if name not in stock_list :
                 del ret
                 continue
              yield name, ret


if __name__ == "__main__" :

   from glob import glob
   import os,sys

   pwd = os.getcwd()
   pwd_ini = pwd.replace('bin','local')

   reader = STOCK_TIMESERIES.init()

   nasdaq = '{}/{}'.format(pwd_ini,NASDAQ.path)
   nasdaq = NASDAQ.init(filename=nasdaq)
   for stock in nasdaq() :
       print stock
       if stock == 'AAPL' : break

   ini_list = glob('{}/*ini'.format(pwd_ini))
   for path, section, key, value in INI.loadList(*ini_list) :
       if 'Industry' not in section : continue
       if 'Gas' not in key : continue
       if 'Util' not in key : continue
       break
   print path
   print section
   print key
   for stock in value :
       ret = reader.extract_from_yahoo(stock)
       print stock
       print ret.describe()
