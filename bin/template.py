#!/usr/bin/env python

import logging
import sys
from libCommon import INI_READ, INI_WRITE
from libUtils import exit_on_exception
from libDebug import trace, cpu

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, config_list, file_list, input_file, output_file) :
        self._env = _env
        self.config_list = config_list
        self.file_list = file_list
        self.input_file = input_file
        self.output_file = output_file
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
        input_file = None
        if len(_env.argv) > 0 :
           input_file = _env.argv[0]
        output_file = None
        if len(_env.argv) > 1 :
           output_file = _env.argv[1]
        target = 'ini_list'
        config_list = globals().get(target,[])
        if not isinstance(config_list,list) :
           config_list = list(config_list)
        target = "file_list"
        file_list = globals().get(target,[])
        cls._singleton = cls(_env,config_list,file_list, input_file, output_file)
        return cls._singleton
    @classmethod
    def config() :
        ini_list = EXTRACT.instance().config_list
        logging.info("loading results {}".format(ini_list))
        for path, section, key, stock_list in INI_READ.read(*ini_list) :
            yield path, section, key, stock_list

class LOAD() :
    @classmethod
    def config(cls, **config) :
        save_file = EXTRACT.instance().output_file
        INI_WRITE.write(save_file,**config)
        logging.info("results saved to {}".format(save_file))

@exit_on_exception
@trace
def main() : 
    logging.info('reading from file {}'.format(EXTRACT.instance().input_file))
    #LOAD.config(**{})

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')

   main()

