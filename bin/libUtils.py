import logging
import os
import sys
from glob import glob
from time import time as _now
from functools import reduce

import feedparser
from itertools import combinations as iter_combo
import requests
from bs4 import BeautifulSoup

from libCommon import load_environ
from libDecorators import exit_on_exception, log_on_exception
from libDecorators import http_200
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

def mkdir(path) :
    if path is None :
       return
    if os.path.exists(path):
       logging.info('Already exists : {}'.format(path))
       return
    logging.info('Creating directory {}'.format(path))
    os.mkdir(path)

class ENVIRONMENT(object) :
      _singleton = None
      @classmethod
      def instance(cls, *largs, **kvargs) :
          if not (cls._singleton is None) :
             return cls._singleton

          ret = cls(*largs, **kvargs)
          ret.__dict__.update(**load_environ())
          ret.__dict__.update(**kvargs)
          target = 'PATH'
          ret.__dict__[target] = ret.__dict__[target].split(':')
          cls._singleton = ret
          return cls._singleton
      def __init__(self, *largs, **kvargs) :
          self.pwd = os.getcwd()
          self.pwd_parent = os.path.dirname(self.pwd)
          self.path = sys.path
          self.name = os.path.basename(sys.argv[0]).split('.')[0]
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
      def mkdir(self, path) :
          if path is None :
              return
          _path = [self.pwd, self.pwd_parent]
          _path = [self.pwd]
          _path = map(lambda x : '{}/{}'.format(x,path),_path)
          _path = list(_path)
          if len(_path) > 0 :
             _path = _path[0]
          mkdir(_path)
      @classmethod
      def parse(cls, *largs, **kvargs) :
          if len(largs) > 0 :
             return largs[0]
          ret = 'extension'
          ret = kvargs.get(ret, '*.*')
          return ret
      @classmethod
      def find(cls, *path_list) :
          for i, path in enumerate(path_list) :
              ret = glob(path)
              if len(ret) > 0 :
                 return sorted(ret)
          return []
      def list_filenames(self, *largs, **kvargs) :
          extension = ENVIRONMENT.parse(*largs, **kvargs)
          path1 = '{}/{}'.format(self.pwd,extension)
          path2 = '{}/{}'.format(self.pwd_parent,extension)
          ret = ENVIRONMENT.find(*[path1, path2])
          return ret

class WEB(object) :
      @classmethod
      @log_on_exception
      def get_content(cls, url,headers=None) :
          ret = cls.get(url,headers)
          return ret.content
      @classmethod
      @log_on_exception
      def get_text(cls, url,headers=None) :
          ret = cls.get(url,headers)
          return ret.text
      @classmethod
      @log_on_exception
      def get_json(cls, url,headers=None) :
          ret = cls.get(url,headers)
          return ret.json()
      @classmethod
      @http_200
      def get(cls, url,headers=None) :
          session = requests.Session()
          logging.debug((url,headers))
          ret = session.get(url, headers=headers)
          if ret is None :
             logging.warning('Nothing returned from http get')
             return ret
          ret.raise_for_status()
          return ret
      @classmethod
      def format_as_soup(cls, url_response, raw=False) :
          if not url_response :
              logging.warning("Can't format None")
              return None
          ret = BeautifulSoup(url_response,'lxml')
          if not raw :
             for script in ret(["script", "style"]):
                 script.extract() # Remove these two elements from the BS4 object
          return ret

class RSS() :
      @classmethod
      def to_dict(cls,rss) :
          feed = feedparser.parse(rss)
          _title = feed['feed']['title']
          yield _title
          for entry in feed.entries :
              yield entry
      @classmethod
      def narrow(cls,*field_list,**entry) :
          logging.info(field_list)
          ret = {}
          for field in field_list :
              ret[field] = entry.get(field,'NA')
          return ret


'''
'''

class DICT_HELPER() :
    def __init__(self, *largs, **kwargs):
        self.data = kwargs
    def __delitem__(self, key):
        value = self.data.pop(key)
        self.data.pop(value, None)
    def __setitem__(self, key, value):
        if key in self.data:
            del self.data[key]
        self.data[key] = value
    def __getitem__(self, key):
        return self.data[key]
    def __str__(self):
        msg = map(lambda x : "{} : {}".format(x,len(self.data[x])), sorted(self.data))
        msg = list(msg)
        msg.append("Total : {}".format(len(self.values())))
        return "\n".join(msg)
    def append(self, key, *value_list):
        if key not in self.data:
           self.data[key] = []
        for i, value in enumerate(value_list) :
            self.data[key].append(value)
    def values(self):
        if len(self.data) == 0 :
           return []
        ret = reduce(lambda a, b : a+b, self.data.values())
        ret = sorted(list(set(ret)))
        return ret
    @classmethod
    def init(cls, *largs, **kwargs) :
        ret = cls(*largs, **kwargs)
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

if __name__ == "__main__" :

   import sys
   import logging

   env = ENVIRONMENT.instance()
   for key in sorted(vars(env)) :
       print((key,vars(env)[key]))
   file_list = env.list_filenames('local/historical_prices/*pkl')

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   ini_list = env.list_filenames('local/*.ini')
   for path, section, key, value in INI_READ.read(*ini_list) :
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

