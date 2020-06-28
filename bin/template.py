#!/usr/bin/env python

import logging
import sys
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

@singleton
class VARIABLES() :
    var_names = [ 'env','config_list', 'stock_data', 'output_file']

    def __init__(self) :
        values = get_globals(*VARIABLES.var_names)
        self.__dict__.update(**values)
        if len(self.env.argv) > 0 :
           self.input_file = self.env.argv[0]
        if len(self.env.argv) > 1 :
           self.output_file = self.env.argv[1]
        msg = vars(self)
        for i, key in enumerate(sorted(msg)) :
            value = msg[key]
            if isinstance(value,list) and len(value) > 10 :
               value = value[:10]
            logging.info((i,key, value))

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

   config_list = env.list_filenames('local/*.ini')
   stock_data = env.list_filenames('local/historical_prices/*pkl')
   output_file = './a.out'

   main()

