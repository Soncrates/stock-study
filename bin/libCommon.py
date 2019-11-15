import csv
import datetime, time
from math import floor
import logging
import os
import sys
from glob import glob
from time import time as _now
import inspect
import functools
from ftplib import FTP as _ftp

from itertools import combinations as iter_combo

if sys.version_info < (3, 0):
   import ConfigParser
else:
    import configparser as ConfigParser

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
   
'''
  Kitchen Sink
  INI - Each program reads the ini file(s) produced by the previous program and produces its own.
        This chaining allows for more rapid development both in execution and debugging.
  NASDAQ - wrapper class around pandas built-in nasdaq reader
         , creates csv of all nasdaq stocks and funds
  TIMER - performance metric tracking
          TODO - create a decorator that can log performance of a function.
  TIME_SERIES - perhaps the only legit class in the entire library
              - defaults to pulling 10 years of stock data
              - Stock data saved as pkl files
'''
def combinations(stock_list,size=5) :
    logging.debug(sorted(stock_list))
    ret_list = iter_combo(stock_list,size)
    for ret in list(ret_list):
        logging.debug(sorted(list(ret)))
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
              logging.info(ret)
              yield symbol_list[1], symbol_list[0], ret

class ENVIRONMENT(object) :
      def __init__(self, *largs, **kvargs) :
          env = os.environ
          self.HOME = env.get( "HOME", None)
          self.LOGNAME = env.get( "LOGNAME", None)
          self.OLDPWD = env.get( "OLDPWD", None)
          self.PATH = env.get( "PATH", None)
          self.PWD = env.get( "PWD", os.getcwd())
          self.USER = env.get( "USER", None)
          self.USERNAME = env.get( "USERNAME", None)
          self.pwd = os.getcwd()
          self.pwd_parent = os.path.dirname(self.pwd)
          self.path = sys.path
          self.name = sys.argv[0].split('.')[0]
          self.argv = sys.argv
          self.version = sys.version
          self.version_info = sys.version_info
      def __str__(self) :
          ret = self.__dict__
          key_list = sorted(ret.keys())
          ret = map(lambda key : "{} : {}".format(key,ret.get(key)), key_list)
          ret = "\n".join(ret)
          return ret
      def list_filenames(self, *largs, **kvargs) :
          if len(largs) > 0 :
             extension = largs[0]
          else :
             target = 'extension'
             extension = kvargs.get(target, '*.*')
          ret = '{}/{}'.format(self.pwd_parent,extension)
          ret = glob(ret)
          if len(ret) == 0 :
             ret = '{}/{}'.format(self.pwd,extension)
             ret = glob(ret)
          ret = sorted(ret)
          logging.debug(ret)
          return ret

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
              logging.debug(ini_file)
              for name, key, value in INI._loadList(ini_file) :
                  yield ini_file, name, key, value

      @staticmethod
      def _loadList(path) :
          if path.endswith('ini') == False : return
          config = INI._init(path)
          for name, key, value in INI.read_section(config) :
              if '{' in value :
                  yield name, key, value
                  continue
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
                  logging.debug((name,key))
                  if len(value) == 0 : continue
                  yield name, key, value

      @staticmethod
      def write_section(config,section,**data) :
          config.add_section(section)
          for key in sorted(data.keys()) :
              logging.debug((section,key))
              value = data[key]
              if isinstance(value,list) :
                 value = ",".join(value)
              config.set(section,key,value)

class FTP:
      get = 'RETR {pwd}'
      @classmethod
      def init(cls, **kwargs) :
          target='server'
          server = kwargs.get(target,'ftp.kernel.org')
          target='user'
          user = kwargs.get(target,None)
          target='pass'
          pswd = kwargs.get(target,None)
          ret = cls(server,user,pswd)
          return ret
      def __init__(self, server,user,pswd):
          self.server = server
          if user is None or pswd is None :
             self.ftp = _ftp(server)
             self.ftp.login()
          else :
             self.ftp = _ftp(server, user, pswd)
          self.data = []
      def __call__(self,s):
          self.data.append(s)
      def __str__(self) :
          return "\n".join(self.data)
      @classmethod
      def GET(cls, obj, **kwargs) :
          obj.data = []
          get = cls.get.format(**kwargs)
          obj.ftp.retrlines(get,obj)
          return obj
      @staticmethod
      def LIST(obj, **kwargs) :
          target = 'pwd'
          pwd = kwargs.get(target, None)
          if pwd is not None :
             obj.ftp.cwd(pwd)
          return obj.ftp.nlst()

class CSV :
      @staticmethod
      def grep(path, *arg_list) :
          ret = {}
          with open(path, 'rb') as csvfile:
               obj = csv.reader(csvfile)
               for row in obj:
                   for local_key in arg_list :
                       flag_list = filter(lambda t : local_key == t, row)
                       if len(flag_list) > 0 :
                          ret[local_key] = row
                          continue
                       alt_key = local_key.replace('-P','-') 
                       flag_list = filter(lambda t : alt_key == t, row)
                       if len(flag_list) == 0 : continue
                       ret[local_key] = row
          return ret

class TIMER :
      minute = 60
      hour = minute*60
      day = hour*24
      order = [ "days", "hours", "minutes" ]
      
      def __init__(self,start) :
          self.start = start
      def __call__(self) :
          return str(self)
      def __str__(self) :
          ret = _now() - self.start
          ret = TIMER.enumerate(ret)
          return TIMER._str(**ret)
      @classmethod
      def init(cls, **kwargs) :
          start = _now()
          return cls(start)
      @classmethod
      def enumerate(cls, elapsed) :
          ret = {}
          ret["days"], elapsed = divmod(elapsed, cls.day)
          ret["hours"], elapsed = divmod(elapsed, cls.hour)
          ret["minutes"], elapsed = divmod(elapsed, cls.minute)
          ret["seconds"] = round(elapsed,2)
          return ret
      @classmethod
      def _str(cls, **units) :
          ret = filter(lambda k : units[k] > 0, cls.order)
          ret = map(lambda k : k + ' : {' + k + '}', ret )
          ret = ', '.join(ret)
          ret += ", seconds : {seconds}"
          return ret.format(**units)

class STOCK_TIMESERIES :
      @classmethod
      def init(cls, **kwargs) :
          target = 'end'
          end = kwargs.get(target, datetime.datetime.utcnow())
          target = 'start'
          start = kwargs.get(target, datetime.timedelta(days=365*10))
          start = end - start
          ret = cls(start,end)
          logging.debug(str(ret))
          return ret
      def __init__(self,start,end) :
          self.start = start
          self.end   = end
      def __str__(self) :
          ret = "{} to {}".format(self.start, self.end)
          return ret
      def extract_from_yahoo(self, stock) :
          ret = self._extract_from(stock, 'yahoo') 
          return ret
      def _extract_from(self, stock, service) :
          try :
             return web.DataReader(stock, service, self.start, self.end) 
          except Exception as e : logging.error(e, exc_info=True)

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
      @staticmethod
      def read_all(file_list, stock_list) :
          name_list = []
          data = None
          for stock_name, stock_data in STOCK_TIMESERIES.read(file_list, stock_list) :
             try :
               name_list.append(stock_name)
               stock_data.columns = pd.MultiIndex.from_product([[stock_name], stock_data.columns])
               if data is None:
                  data = stock_data
               else:
                  data = pd.concat([data, stock_data], axis=1)
             except Exception as e :  logging.error(e, exc_info=True)
             finally : pass
          return name_list, data
      @staticmethod
      def flatten(target,d) :
          d = d.iloc[:, d.columns.get_level_values(1)==target]
          d.columns = d.columns.droplevel(level=1)
          return d

def log_exception(func):
    def exception_guard(*args, **kwargs):
        try:
           return func(*args, **kwargs)
        except Exception as e :
           logging.error(e, exc_info=True)
           sys.exit(e)
    return exception_guard

if __name__ == "__main__" :

   import sys
   import logging

   env = ENVIRONMENT()

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   reader = STOCK_TIMESERIES.init()

   nasdaq = env.list_filenames('local/'+NASDAQ.path)[0]
   nasdaq = NASDAQ.init(filename=nasdaq)
   for stock in nasdaq() :
       print (stock)
       if stock == 'AAPL' : break

   ini_list = env.list_filenames('local/*.ini')
   print(ini_list[0])
   file_list = env.list_filenames('local/historical_prices/*pkl')
   print (file_list[0])
   for path, section, key, value in INI.loadList(*ini_list) :
       if 'Industry' not in section : continue
       if 'Gas' not in key : continue
       if 'Util' not in key : continue
       break
   stock_list = value[:2]
   print (path, section, key, stock_list)

   for stock in stock_list :
       ret = reader.extract_from_yahoo(stock)
       print (stock)
       print (ret.describe())
   a,b = STOCK_TIMESERIES.read_all(file_list, stock_list)
   b = STOCK_TIMESERIES.flatten('Adj Close',b)
   print (b.describe())
   print (a)

   t1 = TIMER.enumerate(313969)
   t2 = TIMER.enumerate(3600)
   print t1
   print t2
   print TIMER._str(**t1)
   print TIMER._str(**t2)
   print(env)


