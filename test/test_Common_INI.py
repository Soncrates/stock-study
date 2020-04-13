#!/usr/bin/python

import logging
import sys
import unittest

import context
from context import test_ini_input_file, test_ini_output_file, test_ini_output_data
from libCommon import INI_READ as READ, INI_WRITE as WRITE, INI_BASE
from libUtils import log_on_exception
from libDebug import trace

class TEST_READ(unittest.TestCase):

    def test_03_read(self) :
        target = 'test_ini_input_file'
        ini_list = globals().get(target,[])
        count = 12
        for a,b,c,d in READ.read(*ini_list) :
            if b.startswith('dep_') :
                continue
            if c.startswith('sharpe') :
                continue
            if c.startswith('risk') :
                continue
            if c.startswith('return') :
                continue
            logging.debug((a,b,c,d))
            self.assertIsInstance(a,str)
            self.assertIsInstance(b,str)
            self.assertIsInstance(c,str)
            if 'dictionary' in c :
               self.assertIsInstance(d,dict)
            else :
               self.assertIsInstance(d,list)
            count -= 1
            if count == 0 : break

class TEST_WRITE(unittest.TestCase):

    def test_03_write(self) :
        target = 'test_ini_output_data'
        data = globals().get(target,{})
        target = 'test_ini_output_file'
        target = globals().get(target,'testResults/error.ini')
        WRITE.write(target, **test_ini_output_data)
        
if __name__ == '__main__' :

   from libUtils import ENVIRONMENT
   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   unittest.main()

