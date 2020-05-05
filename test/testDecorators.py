import logging
import sys
import unittest

import context
from context import test_fund_data_store, test_fund_ticker_list

from libDebug import trace, cpu
from libDecorators import log_on_exception, exit_on_exception
from libDecorators import cache, http_200


@cpu
@trace
def func1(t):
    """sample docstring for test"""
    logging.info("inside func1()")
    return 1
@trace
@cpu
def func2(**kvargs):
    logging.info("inside func2()")
    return 2

class A(object) :
      @cpu
      @trace
      def func3(self,*largs,**kvargs) :
          logging.info("inside func3()")
          return 3

      @trace
      @cpu
      def func4(self, *largs,**kvargs) :
          logging.info("inside func4()")
          return 4

class B(object) :
      @cpu
      @trace
      @staticmethod
      def func5(*largs, **kvargs) :
           logging.info("inside func5()")
           return 5
      @trace
      @cpu
      @staticmethod
      def func6(*largs, **kvargs) :
           logging.info("inside func6()")
           return 6

class TemplateTest(unittest.TestCase):

    def test_01_(self) :
        logging.info(func1(6))
    def test_02_(self) :
        logging.info(func2(test=4))
    def test_03_(self) :
        a = A()
        logging.info(a.func4(4))
    @unittest.skip("TypeError: func3() missing 1 required positional argument: 'self'")
    def test_04_(self) :
        a = A()
        logging.info(a.func3(itest=6))
    @unittest.skip("TypeError: 'staticmethod' object is not callable")
    def test_05_(self) :
        logging.info(B.func5(itest=6))
    @unittest.skip("TypeError: 'staticmethod' object is not callable")
    def test_06_(self) :
        logging.info(B.func6(test=4))
    def test_07_(self) :
        var_names = ['test_fund_ticker_list', 'test_fund_data_store']
        values = get_globals(*var_names)
        logging.info(values)
        
if __name__ == '__main__' :

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   unittest.main()

