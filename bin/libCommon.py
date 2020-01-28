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
   
#import warnings
#warnings.warn("period must be positive", RuntimeWarning)

'''
  Kitchen Sink
  CSV - Some web scraping returns csv files 
  ENVIRONMENT - scraping platform for data (USER,pwd, VERSION etc)
  FTP - web scraping 
  INI - Each program reads the ini file(s) produced by the previous program and produces its own.
        This chaining allows for more rapid development both in execution and debugging.
  TIMER - performance metric tracking
          TODO - create a decorator that can log performance of a function.
'''
def combinations(stock_list,size=5) :
    ret_list = iter_combo(stock_list,size)
    for ret in list(ret_list):
        yield list(ret)

def exit_on_exception(func):
    def exit_program(*args, **kwargs):
        try:
           return func(*args, **kwargs)
        except Exception as e :
           logging.error(e, exc_info=True)
           sys.exit(e)
    return exit_program

def log_on_exception(func):
    def exception_guard(*args, **kwargs):
        ret = None
        try:
           ret = func(*args, **kwargs)
        except Exception as e :
           logging.error(e, exc_info=True)
        finally :
           return ret
    return exception_guard

class ENVIRONMENT(object) :
      def __init__(self, *largs, **kvargs) :
          env = os.environ
          self.HOME = env.get( "HOME", None)
          self.LOGNAME = env.get( "LOGNAME", None)
          self.OLDPWD = env.get( "OLDPWD", None)
          self.PATH = env.get( "PATH", None)
          self.PATH = self.PATH.split(':')
          self.PWD = env.get( "PWD", os.getcwd())
          self.USER = env.get( "USER", None)
          self.USERNAME = env.get( "USERNAME", None)
          self.pwd = os.getcwd()
          self.pwd_parent = os.path.dirname(self.pwd)
          self.path = sys.path
          self.name = sys.argv[0].split('.')[0]
          if len(self.name) == 0 :
             self.name = sys.argv[0].split('.')[1]
          self.argv = sys.argv[1:]
          self.version = sys.version
          self.version_info = sys.version_info
      def __str__(self) :
          values = self.__dict__
          key_list = sorted(values.keys())
          ret = map(lambda key : "{} : {}".format(key,values.get(key)), key_list)
          ret = list(ret)
          ret = "\n".join(ret)
          return ret
      @classmethod
      def parse(cls, *largs, **kvargs) :
          if len(largs) > 0 :
             extension = largs[0]
          else :
             target = 'extension'
             extension = kvargs.get(target, '*.*')
          return extension
      def mkdir(self, path) :
          _path = [self.pwd, self.pwd_parent]
          _path = [self.pwd]
          _path = map(lambda x : '{}/{}'.format(x,path),_path)
          _path = list(_path)
          if len(_path) > 0 :
             _path = _path[0]
          if os.path.exists(_path):
             logging.info('Already exists : {}'.format(_path))
             return
          else :
             logging.info('Creating directory {}'.format(_path))
          os.mkdir(path)
      @classmethod
      def find(cls, path1, path2) :
          ret = glob(path1)
          if len(ret) == 0 :
             ret = glob(path2)
          ret = sorted(ret)
          return ret
      def list_filenames(self, *largs, **kvargs) :
          extension = ENVIRONMENT.parse(*largs, **kvargs)
          path1 = '{}/{}'.format(self.pwd,extension)
          path2 = '{}/{}'.format(self.pwd_parent,extension)
          #logging.info(path1)
          #logging.info(path2)
          ret = ENVIRONMENT.find(path1, path2)
          return ret

class INI(object) :
      @classmethod
      def init(cls) :
          ret = ConfigParser.ConfigParser()
          ret.optionxform=str
          return ret
      @classmethod
      def read(cls, ini_file) :
          config = INI.init()
          ret = open(ini_file)
          config.readfp(ret)
          return config
      @classmethod
      def loadList(cls, *file_list) :
          #file_list = filter(lambda p : p.endswith('ini'), file_list)
          file_list = sorted(file_list)
          for ini_file in file_list :
              for name, key, value in INI._loadList(ini_file) :
                  yield ini_file, name, key, value
      @classmethod
      def _loadList(cls, path) :
          config = INI.read(path)
          for name, key, value in INI.read_section(config) :
              value = INI._transform(value)
              yield name, key, value
      @classmethod
      def _transform(cls, value) :
          if '{' in value :
              # TODO : json.loads
              return value
          if ',' in value :
              value = value.split(',')
              value = map(lambda key : key.strip(), value)
              value = list(value)
              return value
          return [value]
      @classmethod
      def read_section(cls, config) :
          for name in sorted(config._sections) :
              for key, value in config.items(name) :
                  if len(value) == 0 : continue
                  yield name, key, value
      @classmethod
      def _validate(cls, data) :
          if isinstance(data,str) :
             return data
          return str(data)
      @classmethod
      def validate(cls, data) :
          if isinstance(data,list) :
             return ",".join(data)
          return cls._validate(data)
      @classmethod
      def write_section(cls, config,section,**data) :
          config.add_section(section)
          for key in sorted(data.keys()) :
              value = cls.validate(data[key])
              #logging.info((key,type(value)))
              config.set(section,key,value)

class FTP:
      get = 'RETR {pwd}'
      def __init__(self, connection):
          self.connection = connection
          self.data = []
      def __call__(self,s):
          self.data.append(s)
      def __str__(self) :
          return "\n".join(self.data)
      @classmethod
      def init(cls, **kwargs) :
          target='server'
          server = kwargs.get(target,'ftp.kernel.org')
          target='user'
          user = kwargs.get(target,None)
          target='pass'
          pswd = kwargs.get(target,None)
          ret = cls.login(server,user,pswd)
          ret = cls(ret)
          return ret
      @classmethod
      def login(cls, server,user,pswd):
          if user is None or pswd is None :
             ret = _ftp(server)
             ret.login()
          else :
             ret = _ftp(server, user, pswd)
          return ret
      @classmethod
      def GET(cls, obj, **kwargs) :
          obj.data = []
          get = cls.get.format(**kwargs)
          obj.connection.retrlines(get,obj)
          return obj
      @classmethod
      def LIST(cls, obj, **kwargs) :
          target = 'pwd'
          pwd = kwargs.get(target, None)
          if pwd is not None :
             obj.connection.cwd(pwd)
          return obj.connection.nlst()

class CSV :
      @classmethod
      def to_dict(cls, path) :
          logging.info("reading file {}".format(path))
          with open(path, 'rt') as csvfile:
               row_list = csv.reader(csvfile)
               ret = {row[0]:row[1] for row in row_list}
               yield ret
      @classmethod
      def rows(cls, path) :
          logging.info("reading file {}".format(path))
          with open(path, 'rt') as csvfile:
               obj = csv.reader(csvfile)
               for row in obj :
                   yield row
      @classmethod
      def grep(cls, path, *arg_list) :
          ret = {}
          for row in CSV.rows(path) :
              for arg in arg_list :
                  key, _row = CSV._grep(arg, row)
                  if key :
                     ret[key] = _row
          return ret
      @classmethod
      def _grep(cls, key, row) :
          flag = filter(lambda t : key == t, row)
          flag = list(flag)
          if len(flag) > 0 :
             return key, row
          alt_key = key.replace('-P','-') 
          flag = filter(lambda t : alt_key == t, row)
          flag = list(flag)
          if len(flag) > 0 :
             return key, row
          return None, None

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

if __name__ == "__main__" :

   import sys
   import logging

   env = ENVIRONMENT()
   for key in sorted(vars(env)) :
       print((key,vars(env)[key]))
   file_list = env.list_filenames('local/historical_prices/*pkl')

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   ini_list = env.list_filenames('local/*.ini')
   for path, section, key, value in INI.loadList(*ini_list) :
       if 'Industry' not in section : continue
       if 'Gas' not in key : continue
       if 'Util' not in key : continue
       break
   stock_list = value[:2]
   print (path, section, key, stock_list)

   t1 = TIMER.enumerate(313969)
   t2 = TIMER.enumerate(3600)
   print (t1)
   print (t2)
   print (TIMER._str(**t1))
   print (TIMER._str(**t2))
   print (ini_list[0])
   print (file_list[0])
   print(env)

   _nasdaq = env.list_filenames('local/nasdaq.csv')[0]
   row = CSV.grep(_nasdaq, 'AAPL', 'USB') 
   print (row)

