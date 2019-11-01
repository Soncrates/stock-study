#!/usr/bin/python

import logging
import sys
sys.path.append(sys.path[0].replace('test','bin'))
from libCommon import FTP, log_exception
from libDebug import trace

@trace
@log_exception
def main() :
    r = FTP.init(server='ftp.nasdaqtrader.com')
    r = FTP.GET(r, pwd = '/symboldirectory/mfundslist.txt')
    logging.debug(r)
    n = FTP.LIST(r, pwd = 'symboldirectory')
    logging.info(n)

if __name__ == '__main__' :

   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   main()

