# ./libCommon.py
from glob import glob
from json import dumps, load, loads
import logging as log
from os import path, environ, remove, stat, mkdir as MKDIR
import logging
import pandas as PD
from sys import version_info
from time import time, sleep
from traceback import print_exc

try :
   if version_info.major ==3 :
       from configparser import RawConfigParser as CF
   else:
      from ConfigParser import RawConfigParser as CF
except :
    print_exc()
if version_info < (3, 0):
   import ConfigParser
else:
    import configparser as ConfigParser
      
LOG_FORMAT_TEST = '%(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s'
LOG_FORMAT_APP = '[%(asctime)] %(levelname)s [%(module)s.%(funcName)s:%(lineno)d] %(message)s'
LOG_FORMAT_DATE = "%Y%m%dT%"
LOG = logging.getLogger(__name__) 

def find_files(path_name) :
    return glob('{}*'.format(str(path_name).strip('*')))
def remove_file(filename) :
    if not path.exists(filename) : return
    remove(filename)
def mkdir(pathname) :
    if pathname is None : 
         raise ValueError("Object is null")
    if path.exists(pathname):
       LOG.info('Already exists {}'.format(pathname))
       return
    LOG.info('Creating directory {}'.format(pathname))
    MKDIR(pathname)
def load_environ() :
     return { key : environ[key] for key in environ if is_environ(key) }
def is_environ(arg) :
    if 'SUDO' in arg : return True
    if 'NAME' in arg : return True
    if 'USER' in arg : return True
    if 'PATH' in arg : return True
    if 'PWD' in arg : return True
    if 'HOME' in arg : return True
    return False  
def pretty_print(obj) :
    _xx = transform_obj(obj)
    _yy = { key : _xx[key] for key in _xx if not is_json_enabled(_xx[key]) }

    ret = { key : _xx[key] for key in _xx if is_json_enabled(_xx[key]) }
    ret.update( { key : str(_yy[key]) for key in _yy if hasattr(_yy[key],'__str__') } )
    ret.update( { key : str(type(_yy[key])) for key in _yy if not hasattr(_yy[key],'__str__') } )
    return dumps( transform_obj(obj)  , indent=3, sort_keys=True)
def is_json_enabled(obj) :
    try :
        dumps(obj)
        return True
    except : pass
    return False
def transform_obj(obj) :
    if obj is None :
        raise ValueError('Object is None')
    if isinstance(obj,(float, int, str, dict, tuple)) : 
        return obj
    if isinstance(obj,list) :
        return [ transform_obj(arg) for arg in obj if is_str(arg) ]
    if hasattr(obj,'sections') and hasattr(obj,'items') :
       return { section : { key : value for (key,value) in obj.items(section) } for section in obj.sections() }
    prop_list = [ key for key in dir(obj) if not key.startswith("__") and _build_arg(getattr(obj,key)) ]
    return { key : transform_obj(getattr(obj,key))  for key in prop_list }
def build_args(*largs) :
    return "".join( [ str(arg).strip(' ') for arg in largs if is_str(arg) ] )
def build_command(*largs) :
    return "/".join( [ str(arg).strip('/') for arg in largs if is_str(arg) ] )
def build_path(*largs) :
    return " ".join( [ str(arg).strip(' ') for arg in largs if is_str(arg) ] )
def is_str(arg) :
    if arg is None : return False
    if callable(arg) : return False
    if hasattr(arg,'__str__') : return True
    return True
def find_subset(obj,*largs) :
    if obj is None :
       raise ValueError("obj is NoneType")
    if isinstance(obj, dict) :
       return { key: obj[key] for key in largs if key in obj }
    if isinstance(obj,PD.DataFrame) :
        columns = obj.columns.values.tolist()
        omit = { key for key in columns if key not in largs }
        ret = obj.drop(omit,axis=1)
        return ret
    return { key : getattr(obj,key) for key in largs if key in hasattr(obj.key) }
def load_config(fileName) :
    config = CF()
    config.read(fileName)
    log.debug(config)
    return transform_obj(config)
def load_json(fileName) :
    with open(glob(fileName)[0]) as fp :
        return load(fp)
def iterate_config(config) :
    log.debug(config)
    ret = transform_obj(config)
    log.info(ret)
    for i, section in enumerate(sorted(ret)) :
        log.info((section,type(ret[section])))
        for j, key in enumerate(sorted(ret[section])) :
            log.debug(type(key))
            log.debug(key)
            yield i,j, section, key, ret[section][key]
def dict_append_list(ret, key, *value_list):
    if not isinstance(ret,dict) :
       return ret
    if not key or len(key) == 0 :
       return ret
    if not value_list or len(value_list) == 0 :
       return ret
    if key not in ret:
       ret[key] = []
    ret[key].extend(value_list)
    return ret
def dump_ticker_name(ret) :
    if not isinstance(ret,str) :
       return ret
    return ret.replace('%', '_pct_').replace('=', '_eq_')
def load_ticker_name(ret) :
    if not isinstance(ret,str) :
       return ret
    return ret.replace('_pct_','%').replace('_eq_','=')
def pre_load_ticker_name(ret) :
    ret = ret.strip()
    if ret.startswith('{') and ret.endswith('}') :
       return load(ret.replace("'",'"').replace("`","'"))
    return [ arg.strip() for arg in ret.split(',') ]
def pre_dump_ticker_name(ret) :
    if isinstance(ret,list) :
       return ",".join(ret)
    if isinstance(ret,dict) :
       return dumps(ret)
    if isinstance(ret,str) :
       return ret.replace("'","`").replace('"',"'")
    return str(ret)

            
import csv
import logging
from ftplib import FTP as _ftp

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

class INI_READ(object) :
      @classmethod
      def read(cls, *file_list) :
          file_list = [ load_config(arg) for arg in file_list]
          for i, ini_file in enumerate(file_list) :
              log.info('Reading results : {}'.format(ini_file))
              for i,j, section, key, value in iterate_config(file_list[i]) :
                  key = load_ticker_name(key)
                  value = pre_load_ticker_name(value)
                  value = load_ticker_name(value)
                  yield section, key, value
              log.info('Read results : {}'.format(ini_file))
                  
class INI_WRITE(object) :
      @classmethod
      def write(cls, filename,**data) :
          filename = filename.replace(' ','SPACE')
          log.info('Writing results : {}'.format(filename))
          logging.debug(data)
          config = INI_BASE.init()
          #cls.write_ini(config,**data)
          for i,j, section, key, value in iterate_config(data) :
              value = pre_dump_ticker_name(value)
              value = dump_ticker_name(value)
              key = dump_ticker_name(key)
              if not config.has_section(section) :
                  config.add_section(section)
              config.set(section,key,value)
          fp = open(filename, 'w')
          config.write(fp)
          fp.close()
          log.info('Saved results : {}'.format(filename))
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

   logging.basicConfig(stream=sys.stdout, format=LOG_FORMAT_TEST, level=logging.DEBUG)
