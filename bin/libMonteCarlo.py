import pandas as pd
import numpy as np
import warnings
import logging

from libSharpe import HELPER, PORTFOLIO
'''
   Monte Carlo simulations are named after the gambling hot spot in Monaco.

   There are two components to an asset's price movements: 
         drift, which is a constant directional movement, 
         random input, which represents market volatility. 
   By analyzing historical price data, you can determine the drift, standard deviation, variance, and average price movement for a security. 

   Volatility can be measured using two different methods. 
   First is based on performing statistical calculations on the historical prices over a specific time period. 
   This process involves computing various statistical numbers, like mean (average), variance and finally the standard deviation on the historical price data sets.

'''
class MonteCarlo(object) :
      def __init__(self, period) :
          self.period = period
      def __call__(self, stocks,data, num_portfolios = 25000) :
          return PORTFOLIO.find(data, stocks=stocks, portfolios=num_portfolios,period=self.period)
      def findSharpe(self, data, risk_free_rate=0.02) :
          data.sort_index(inplace=True)
          ret = HELPER.find(data, risk_free_rate=risk_free_rate, period=self.period, span=0)
          return ret 
      @classmethod
      def YEAR(cls) :
          ret = cls(252) 
          return ret
      @classmethod
      def QUARTER(cls) :
          ret = cls(63) 
          return ret
      @classmethod
      def MONTH(cls) :
          ret = cls(21) 
          return ret

if __name__ == "__main__" :
   
   import sys

   from libFinance import STOCK_TIMESERIES

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

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
