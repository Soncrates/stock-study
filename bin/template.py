#!/usr/bin/env python

import logging
import sys
from libCommon import exit_on_exception
from libDebug import trace, cpu

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, config_list, file_list, input_file, output_file) :
        self._env = _env
        self.config_list = config_list
        self.file_list = file_list
        self.input_file = input_file
        self.output_file = output_file
    @classmethod
    def singleton(cls, **kwargs) :
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
class LOAD() :
    @classmethod
    def config(cls, **config) :
        save_file = EXTRACT.singleton().output_file
        ret = INI.init()
        for key in sorted(config) :
            value = config.get(key,[])
            logging.info(value)
            INI.write_section(ret,key,**value)
        ret.write(open(save_file, 'w'))
        logging.info("results saved to {}".format(save_file))

@exit_on_exception
@trace
def main() : 
    logging.info('reading from file {}'.format(EXTRACT.singleton().input_file))
    #LOAD.config(**{})

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')

   main()

