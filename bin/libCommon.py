import csv
import logging
#import os
import sys
from ftplib import FTP as _ftp
from json import dumps, loads

if sys.version_info < (3, 0):
   import ConfigParser
else:
    import configparser as ConfigParser
   
#import warnings
#warnings.warn("period must be positive", RuntimeWarning)

'''
  Kitchen Sink
  CSV - Some web scraping returns csv files 
  FTP - web scraping 
  INI - Each program reads the ini file(s) produced by the previous program and produces its own.
        This chaining allows for more rapid development both in execution and debugging.
'''

'''
'''
class INI_BASE(object) :
      @classmethod
      def init(cls) :
          ret = ConfigParser.ConfigParser()
          ret.optionxform=str
          return ret
      @classmethod
      def dump_name(cls, ret) :
          ret = ret.replace('%', '_pct_')
          ret = ret.replace('=', '_eq_')
          return ret
      @classmethod
      def load_name(cls, ret) :
          ret = ret.replace('_pct_','%')
          ret = ret.replace('_eq_','=')
          return ret

      @classmethod
      def load(cls, ret) :
          ret = ret.strip()
          if ret.startswith('{') and ret.endswith('}') :
             ret = ret.replace("'",'"')
             ret = ret.replace("`","'")
             ret = loads(ret)
             return ret
          if ',' not in ret :
             return [ret]
          ret = ret.split(',')
          ret = map(lambda key : key.strip(), ret)
          return list(ret)
      @classmethod
      def _dump(cls, ret) :
          if not isinstance(ret,str) :
            if isinstance(ret,dict) :
               ret = dumps(ret)
            else :
               ret = str(ret)
          ret = ret.replace("'","`")
          ret = ret.replace('"',"'")
          return ret
      @classmethod
      def dump(cls, ret) :
          if isinstance(ret,list) :
             return ",".join(ret)
          return cls._dump(ret)

class INI_READ(object) :
      @classmethod
      def read(cls, *file_list) :
          for ini_file in sorted(file_list) :
              for name, key, value in cls.read_ini(ini_file) :
                  yield ini_file, name, key, value
      @classmethod
      def read_ini(cls, path) :
          fp = open(path)
          config = INI_BASE.init()
          config.read_file(fp)
          for name, key, value in cls.read_section(config) :
              key = INI_BASE.load_name(key)
              value = INI_BASE.load(value)
              yield name, key, value
          fp.close()
      @classmethod
      def read_section(cls, config) :
          for name in sorted(config._sections) :
              for key, value in config.items(name) :
                  if len(value) == 0 : continue
                  yield name, key, value

class INI_WRITE(object) :
      @classmethod
      def write(cls, filename,**data) :
          config = INI_BASE.init()
          cls.write_ini(config,**data)
          fp = open(filename, 'w')
          config.write(fp)
          fp.close()
      @classmethod
      def write_ini(cls, config,**data) :
          for section in sorted(data.keys()) :
              values = data.get(section,{})
              cls.write_section(config,section,**values)
      @classmethod
      def write_section(cls, config,section,**data) :
          config.add_section(section)
          for key in sorted(data.keys()) :
              value = INI_BASE.dump(data[key])
              key = INI_BASE.dump_name(key)
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
               row_list = csv.DictReader(csvfile)
               #ret = {row[0]:row[1] for row in row_list}
               for row in row_list :
                   yield row
      @classmethod
      def rows(cls, path) :
          logging.info("reading file {}".format(path))
          with open(path, 'rt') as csvfile:
               row_list = csv.reader(csvfile)
               for row in row_list :
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

if __name__ == "__main__" :

   import sys
   import logging

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)
