#!/usr/bin/python

import logging
import unittest

import context
from libCommon import FTP as TEST
from libUtils import log_on_exception
from libDebug import trace

class TemplateTest(unittest.TestCase):

    def test_01_download_file(self) :
        target = 'test_server'
        target = globals().get(target,None)
        r = TEST.init(server=target)

        target = 'test_file'
        target = globals().get(target,None)
        ret = TEST.GET(r, pwd = target)

        logging.debug(ret)
    def test_02_list_remote_directories(self) :
        target = 'test_server'
        target = globals().get(target,None)
        r = TEST.init(server=target)

        target = 'test_dir'
        target = globals().get(target,None)
        ret = TEST.LIST(r, pwd = 'symboldirectory')

        logging.info(ret)

if __name__ == '__main__' :

   import sys
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   test_server = 'ftp.nasdaqtrader.com'
   test_file = '/symboldirectory/mfundslist.txt'
   test_dir = 'symboldirectory'
   unittest.main()

