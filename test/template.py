#!/usr/bin/python

import sys
sys.path.append(sys.path[0].replace('test','bin'))
import logging

from libCommon import log_exception
from libDebug import trace

@log_exception
@trace
def main() : 
    target = 'ini_list'
    ini_list = globals().get(target,[])
    logging.info(ini_list)
    target = 'file_list'
    file_list = globals().get(target,[])
    logging.info(file_list)

if __name__ == '__main__' :

   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')

   main()

