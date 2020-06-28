#!/usr/bin/env python

import argparse
import logging
import sys
import time
from libCommon import INI_READ, INI_WRITE
from libDecorators import exit_on_exception, singleton
from libDebug import trace, cpu

@exit_on_exception
def get_globals(*largs) :
    ret = {}
    for name in largs :
        value = globals().get(name,None)
        if value is None :
           raise ValueError(name)
        ret[name] = value
    return ret
def pprint(msg) :
    for i, key in enumerate(sorted(msg)) :
        value = msg[key]
        if isinstance(value,list) and len(value) > 10 :
           value = value[:10]
        logging.info((i,key, value))

@singleton
class VARIABLES() :
    var_names = [ 'env','config_list', 'stock_data','sys_args']

    def __init__(self) :
        values = get_globals(*VARIABLES.var_names)
        self.__dict__.update(**values)
        self.__dict__.update(**vars(self.sys_args))
        pprint(vars(self))
        time.sleep(5)

class EXTRACT() :
    config_list = {}
    @classmethod
    def read(cls, *path_of_interest) :
        ini_list = cls.config_list
        if len(path_of_interest) > 0 :
            ini_list = filter(lambda x : x in path_of_interest, ini_list)
            ini_list = list(ini_list)
        logging.info("loading results {}".format(ini_list))
        for path, section, key, value in INI_READ.read(*ini_list) :
            logging.debug((path, section, key, value))
            yield path, section, key, value

class LOAD() :
    output_file = None
    @classmethod
    def config(cls, **config) :
        save_file = cls.output_file
        INI_WRITE.write(save_file,**config)
        logging.info("results saved to {}".format(save_file))

@exit_on_exception
@trace
def main() : 
    EXTRACT.config_list = VARIABLES().config_list
    path_list = ['/sandbox/repo/stock-study/local/stock_background.ini', '/sandbox/repo/stock-study/local/stock_by_sector.ini']
    LOAD.output_file = VARIABLES().output_file
    return
    for path, section, key, value in EXTRACT.read(*path_list) : pass
    #LOAD.config(**{})

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   parser = argparse.ArgumentParser(description='Example arguments')
   parser.add_argument('--portfolio', action='store', dest='portfolio', type=int, default=100, help='Store a simple value')
   parser.add_argument('--out', action='store', dest='output_file', default='./a.out',help='Store a simple value')
   parser.add_argument('--risk', action='store', dest='risk', type=int, default=10, help='Store a simple value')
   parser.add_argument('--returns', action='store', dest='returns', type=int, default=4, help='Store a simple value')
   sys_args = parser.parse_args()
   pprint(vars(sys_args))

   config_list = env.list_filenames('local/*.ini')
   stock_data = env.list_filenames('local/historical_prices/*pkl')

   main()

