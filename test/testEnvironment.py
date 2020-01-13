#!/usr/bin/python
import sys
sys.path.append(sys.path[0].replace('test','bin'))
import logging

from libCommon import log_exception
from libDebug import trace

@log_exception
@trace
def main() :
    env = ENVIRONMENT()
    logging.info(env)
    file_list = env.list_filenames(extension='local/*.ini')
    logging.info(file_list)

if __name__ == '__main__' :

   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   main()

