import logging
import sys

import context
from libDebug import trace, cpu

if __name__ == '__main__' :

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   @cpu
   @trace
   def func1(t):
       """sample docstring for test"""
       print("inside func1()")
       return 1

   @trace
   @cpu
   def func2(**kvargs):
       print("inside func2()")
       return 2

   print(func1(6))
   print(func2(test=4))
   '''
   class B(object) :
       @cpu
       @trace
       @staticmethod
       def func5(**kvargs) :
           print("inside func5()")
           return 5
       @trace
       @cpu
       @staticmethod
       def func6(**kvargs) :
           print("inside func6()")
           return 6
   print(B.func5(itest=6))
   print(B.func6(test=4))
   '''
   class A(object) :
       @cpu
       @trace
       def func3(self,**kvargs) :
           print("inside func3()")
           return 3

       @trace
       @cpu
       def func4(self, **kvargs) :
           print("inside func4()")
           return 4

   a = A()
   print(a.func4(4))
   print(a.func3(itest=6))
