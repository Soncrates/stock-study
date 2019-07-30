import pandas as pd
from pandas import DataFrame as df
import matplotlib.pyplot as plt

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
def main(file_list, stock_list) :
    for path in file_list :
        name, ret = load_file(path)
        if name not in stock_list :
           del ret
           continue
        yield name, ret 

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

if __name__ == "__main__" :
   from glob import glob
   import os,sys

   pwd = os.getcwd()
   pwd = pwd.replace('/test','')
   ini_list = glob('{}/*.ini'.format(pwd))
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))
   file_list = sorted(file_list)
    
   stock_list = ["SPY", "HD", "PYPL", "SBUX", "UNH", "WEC"]
   target_01 = 'Adj Close'
   target_02 = 'Volume'
   stocks = {}
   for name, data in main(file_list, stock_list) :
       data = data[[target_01,target_02]]
       stocks[name] = data
       pct = data.pct_change()

   STEP_01.returns(target_01, **stocks)
   save("{}/step_01_{}.png".format(pwd,target_01))
   STEP_02.returns(target_01, **stocks)
   save("{}/step_02_{}.png".format(pwd,target_01))
   STEP_03.returns(target_01, **stocks)
   save("{}/step_03_{}.png".format(pwd,target_01))
   STEP_04.returns(target_01, **stocks)
   save("{}/step_04_{}.png".format(pwd,target_01))
   STEP_01.volume(target_02, **stocks)
   save("{}/step_01_{}.png".format(pwd,target_02))
   STEP_02.volume(target_02, **stocks)
   save("{}/step_02_{}.png".format(pwd,target_02))
   STEP_03.volume(target_02, **stocks)
   save("{}/step_03_{}.png".format(pwd,target_02))
   STEP_04.volume(target_02, **stocks)
   save("{}/step_04_{}.png".format(pwd,target_02))
