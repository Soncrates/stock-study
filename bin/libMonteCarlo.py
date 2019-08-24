import pandas as pd
import numpy as np
import warnings
import logging

from libSharpe import HELPER, PORTFOLIO

class MonteCarlo(object) :
      @staticmethod
      def YEAR() :
          ret = MonteCarlo(252) 
          return ret
      @staticmethod
      def QUARTER() :
          ret = MonteCarlo(63) 
          return ret
      @staticmethod
      def MONTH() :
          ret = MonteCarlo(21) 
          return ret
      def __init__(self, period) :
          self.period = period
      def findSharpe(self, data, risk_free_rate=0.02) :
          data.sort_index(inplace=True)
          ret = HELPER.find(data, risk_free_rate=risk_free_rate, period=self.period, span=0)
          return ret 
      def __call__(self, stocks,data, num_portfolios = 25000) :
          return PORTFOLIO.find(data, stocks=stocks, portfolios=num_portfolios,period=self.period)

if __name__ == "__main__" :
   
   from glob import glob
   import os,sys

   from libCommon import STOCK_TIMESERIES

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)

   target = 'Adj Close'
   stock_list = ['AMZN','CMS','DOV','DTE','EQR','HD','LHX','MA','MMC','NKE','NOC','PYPL','QLD','SBUX','UNH','UNP','WEC']
   stock_list = stock_list[2::3]

   # debugging
   stock_list = ['DIAL', 'IMFI', 'FISR', 'WBIN']
   stock_list = ['EVBG', 'MDB', 'OKTA', 'SHOP']

   reader = STOCK_TIMESERIES.init()
   annual = MonteCarlo.YEAR()

   stock_data = pd.DataFrame()
   for stock in stock_list :
       data = reader.extract_from_yahoo(stock)

       d = annual.findSharpe(data[target])
       print d
       ret = d.get('returns', 0)
       risk = d.get('risk', 0)
       sharpe = d.get('sharpe', 0)
       length = d.get('len', 0)
       ret = round(ret,2)
       risk = round(risk,2)
       sharpe = round(sharpe,2)
       print "{} return {}, risk {}, sharpe {}, length {}".format(stock,ret,risk,sharpe,length)
       if sharpe == 0 : 
          continue
       stock_data[stock] = data[target]

   max_sharpe, min_vol = annual(stock_list,stock_data)
   max_sharpe = max_sharpe.round(2)
   min_vol = min_vol.round(2)
   print max_sharpe
   print min_vol

