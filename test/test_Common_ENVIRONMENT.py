#!/usr/bin/python
import logging
import unittest

import context
from libCommon import ENVIRONMENT as TEST

class TemplateTest(unittest.TestCase):

    def test_01_default(self) :
        target = 'test'
        test = globals().get(target,globals())
        logging.info(test)
    def test_02_local(self) :
        target = 'test_files'
        test = globals().get(target,globals())
        logging.info(test)
    def test_03_higher(self) :
        target = 'file_list'
        test = globals().get(target,globals())
        logging.info(test)

if __name__ == '__main__' :

   import sys
   import logging

   test = TEST.instance()
   file_list = test.list_filenames(extension='local/*.ini')
   test_files = test.list_filenames('testConfig/*.ini')
   #log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(test))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)
   unittest.main()


