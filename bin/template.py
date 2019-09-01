#!/usr/bin/python

import logging

def main(file_list, ini_list) :
    try :
        return _main(file_list, ini_list)
    except Exception as e :
        logging.error(e, exc_info=True)

def _main(file_list, ini_list) : pass

if __name__ == '__main__' :

   from glob import glob
   import os,sys
   from libCommon import TIMER

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   local = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(local))
   file_list = glob('{}/historical_prices/*pkl'.format(local))

   logging.info("started {}".format(name))
   elapsed = TIMER.init()
   main(file_list,ini_list)
   logging.info("finished {} elapsed time : {}".format(name,elapsed()))

