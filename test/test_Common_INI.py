#!/usr/bin/python

import logging
import sys
import unittest

import context
from libCommon import INI_READ as READ, INI_WRITE as WRITE, INI_BASE
from libUtils import log_on_exception
from libDebug import trace

class TEST_READ(unittest.TestCase):

    def test_03_(self) :
        target = 'read_many'
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

    data = { 'stanza 1 simple' : {'string' : 'I am a string', 'string with single quote' : "I don't like single quotes", 'int0' : 0, 'int2' : 2, 'int negative' : -2}
            , 'stanza 2 complex' : { 'list' : ['a','b','c']
                , 'dictionary' : { 'letters' : ['a','b','c']
                    , 'numbers' : [0,1,2]
                    , 'single quote' : "I don't like single quotes"} 
                } 
            }
    def test_03_(self) :
        target = 'write'
        target = globals().get(target,'error.ini')
        WRITE.write(target, **TEST_WRITE.data)
        
@log_on_exception
@trace
def prep() : 
    target = 'read_one'
    read_one = globals().get(target,[])
    logging.info(read_one)
    target = 'read_many'
    read_many = globals().get(target,[])
    logging.info(read_many)

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT
   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   read_one = env.list_filenames('testConfig/refined_stock_list.ini')
   read_many = env.list_filenames('testConfig/test*.ini')
   write = 'testResults/testWrite.ini'

   prep()
   unittest.main()

