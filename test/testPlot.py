import sys

import pandas as pd
from pandas import DataFrame as df
import matplotlib.pyplot as plt

import context
from context import test_plot_dir, test_plot_stock_list

def load_file(filename) :
    data = pd.read_pickle(filename)
    target = 'Stock'
    if target in data :
       name = data.pop(target)
       name = name[0]
       return name, data
    name = filename.split("/")[-1]
    name = name.split(".")[0]
    return name, data
class STEP_01 :
      @staticmethod
      def returns(label,**stocks) :
          for key in stocks.keys() :
              ret = stocks[key]
              ret[label].plot(label="{}".format(key))
      @staticmethod
      def volume(label,**stocks) :
          for key in stocks.keys() :
              ret = stocks[key]
              ret[label].plot(label="{}".format(key))
class STEP_02 :
      @staticmethod
      def returns(label, **stocks) :
          for key in stocks.keys() :
              ret = stocks[key]
              ret = ret.pct_change()
              ret[label].plot(label="{}".format(key))
      @staticmethod
      def volume(label,**stocks) :
          for key in stocks.keys() :
              ret = stocks[key]
              ret = ret.pct_change()
              ret[label].plot(label="{}".format(key))
class STEP_03 :
      @staticmethod
      def returns(label,**stocks) :
          for key in stocks.keys() :
              ret = stocks[key]
              ret = ret.pct_change()
              ret = ret.cumsum()
              ret[label].plot(label="{}".format(key))
      @staticmethod
      def volume(label,**stocks) :
          for key in stocks.keys() :
              ret = stocks[key]
              ret = ret.pct_change()
              ret = ret.cumsum()
              ret[label].plot(label="{}".format(key))
class STEP_04 :
      @staticmethod
      def returns(label,**stocks) :
          for key in stocks.keys() :
              ret = stocks[key]
              ret = ret.apply(lambda x: x / x[0])
              ret[label].plot(label="{}".format(key))
      @staticmethod
      def volume(label,**stocks) :
          for key in stocks.keys() :
              ret = stocks[key]
              ret = ret.apply(lambda x: x / x[0])
              ret[label].plot(label="{}".format(key))
def save(path) :
   plt.legend()
   plt.savefig(path)
   plt.clf()
   plt.cla()
   plt.close()

def action(stock_list) :
    target = 'file_list'
    file_list = globals().get(target,[])
    for path in file_list :
        name, ret = load_file(path)
        if name not in stock_list :
           del ret
           continue
        yield name, ret 
    
def main() :
   target = 'test_plot_dir'
   local_dir = globals().get(target,None)
   target = 'test_plot_stock_list'
   stock_list = globals().get(target,None)

   target_01 = 'Adj Close'
   target_02 = 'Volume'
   stocks = {}
   for name, data in action(stock_list) :
       data = data[[target_01,target_02]]
       stocks[name] = data
       pct = data.pct_change()

   STEP_01.returns(target_01, **stocks)
   save("{}/step_01_{}.png".format(local_dir,target_01))
   STEP_02.returns(target_01, **stocks)
   save("{}/step_02_{}.png".format(local_dir,target_01))
   STEP_03.returns(target_01, **stocks)
   save("{}/step_03_{}.png".format(local_dir,target_01))
   STEP_04.returns(target_01, **stocks)
   save("{}/step_04_{}.png".format(local_dir,target_01))
   STEP_01.volume(target_02, **stocks)
   save("{}/step_01_{}.png".format(local_dir,target_02))
   STEP_02.volume(target_02, **stocks)
   save("{}/step_02_{}.png".format(local_dir,target_02))
   STEP_03.volume(target_02, **stocks)
   save("{}/step_03_{}.png".format(local_dir,target_02))
   STEP_04.volume(target_02, **stocks)
   save("{}/step_04_{}.png".format(local_dir,target_02))

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   main()

