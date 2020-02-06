import logging
import warnings
#warnings.warn("period must be positive", RuntimeWarning)
try:
    xrange
except NameError:
    xrange = range

import numpy as np
import pandas as pd

from libFinance import HELPER as FINANCE
from libDebug import cpu
'''
Sharpe Ratio

(Portfollio Expected Return - Risk Free Rate) / Portfolio Risk
Expected Return a.k.a Mean
Risk a.k.a Standard Deviation

The Sharpe ratio, also known as the reward-to-variability ratio, is perhaps the most common portfolio management metric. 

The excess return of the portfolio over the risk-free rate is standardized by the risk of the excess of the portfolio return. 

Hypothetically, investors should always be able to invest in government bonds and obtain the risk-free rate of return. 

The Sharpe ratio determines the expected realized return over that minimum. 

Within the risk-reward framework of portfolio theory, higher risk investments should produce high returns. 

As a result, a high Sharpe ratio indicates superior risk-adjusted performance.

'''
'''
class SHARPE : unused, 
class HELPER :
class PORTFOLIO :

'''

class PORTFOLIO :
      columns = ['returns','risk','sharpe']

      @classmethod
      def validate(cls, data, **kwargs) :
          target = "stocks"
          stocks = kwargs.get(target,['AAPL','GOOG'])
          target = "portfolios"
          portfolios = kwargs.get(target,25000)
          target = "period"
          period = kwargs.get(target,252)
          target = "risk_free_rate"
          risk_free_rate = kwargs.get(target,0.02)
          if portfolios < 0 :
             logging.warn("portfolios must be positive")
             portfolios = 0
          if period < 0 :
             logging.warn("period must be positive")
             period = 0
          if risk_free_rate < 0 :
             logging.warn("risk_free_rate must be positive")
             risk_free_rate = 0
          if data is None :
             logging.warn('No data!')
             return data, stocks, portfolios, risk_free_rate, period
          logging.debug(stocks)
          #ticker_list = data.index.values
          ticker_list = data.columns.values
          ticker_list = list(ticker_list)
          logging.debug(ticker_list)
          stocks = filter(lambda x : x in ticker_list, stocks)
          if not isinstance(stocks,list) :
             stocks = list(stocks)
          flag = len(stocks) == 0
          if flag :
             data = None 
          else :
             #data = data.filter(items=stocks,axis='index')
             data = data.filter(items=stocks,axis='columns')
          logging.debug(data)
          return data, stocks, portfolios, risk_free_rate, period
      @classmethod
      @cpu
      def findWeightedSharpe(cls, data, weights, risk_free_rate=0.02, period=252) :
          if not isinstance(data,pd.DataFrame) :
             logging.warn("prices are not in a dataframe {}".format(type(data)))
             data = pd.DataFrame(data)

          #calculate mean daily return and covariance of daily returns
          mean = data.mean()
          cov_matrix = data.cov()
          returns, risk, sharpe = cls._sharpe(cov_matrix, mean, period, risk_free_rate, weights)
          ret = dict(zip(['returns', 'risk', 'sharpe'],[returns,risk,sharpe]))
          logging.info(ret)
          return ret
      @classmethod
      def _weights(cls, size, num_portfolios) :
          low = 0.1
          high = low + low + (1/size) 
          for i in xrange(num_portfolios):
              #select random weights for portfolio holdings
              weights = np.random.uniform(low=low, high=high, size=size)
              weights = np.array(weights)
              #rebalance weights to sum to 1
              weights /= np.sum(weights)
              yield weights, i

      @classmethod
      def _sharpe(cls, cov_matrix, mean, period, risk_free_rate, weights) :

          magic = np.dot(cov_matrix, weights)
          magic_number = np.dot(weights.T,magic)

          #calculate return and volatility
          returns = np.sum(mean * weights) * period
          risk = np.sqrt(magic_number) * np.sqrt(period)

          #calculate Sharpe Ratio (return - risk free rate / volatility)
          sharpe = 0
          if risk != 0 : 
             sharpe = ( returns - risk_free_rate ) / risk
          return returns, risk, sharpe

      @classmethod
      def transformReturns(cls, returns) :
          ret = FINANCE.findDailyReturns(returns)
          mean = ret.mean()
          cov_matrix = ret.cov()
          #logging.info(cov_matrix)
          #logging.info(ret)
          #logging.info(mean)
          return ret, mean, cov_matrix

      @classmethod
      def _find(cls, data, stocks, num_portfolios, risk_free_rate, period) :

          #set up array to hold results
          #We have increased the size of the array to hold the weight values for each stock
          size = len(stocks)
          ret = np.zeros((3+size,num_portfolios))

          returns, mean, cov_matrix = cls.transformReturns(data)
          for weights, i in cls._weights(size, num_portfolios) :
              returns, risk, sharpe = cls._sharpe(cov_matrix, mean, period, risk_free_rate, weights)
              #store results in results array
              ret[0,i] = returns
              ret[1,i] = risk
              ret[2,i] = sharpe
              for j in range(len(weights)):
                  ret[j+3,i] = weights[j]

          #convert results array to Pandas DataFrame
          columns = cls.columns + stocks
          ret = pd.DataFrame(ret.T,columns=columns)
          logging.debug(ret.head(3))
          logging.debug(ret.tail(3))
          return ret

      @classmethod
      def find(cls, data, **kwargs) :
          data, stocks, num_portfolios, risk_free_rate, period = cls.validate(data, **kwargs)
          if data is None :
              return pd.DataFrame(), pd.DataFrame()

          ret = cls._find(data, stocks, num_portfolios, risk_free_rate, period)

          #locate position of portfolio with highest Sharpe Ratio
          max_sharpe = ret['sharpe'].idxmax()
          max_sharpe_port = ret.iloc[max_sharpe]

          #locate positon of portfolio with minimum risk
          min_vol = ret['risk'].idxmin()
          min_vol_port = ret.iloc[min_vol]
          return max_sharpe_port, min_vol_port

if __name__ == "__main__" :

   import sys
   import logging

   from libCommon import ENVIRONMENT, INI
   from libFinance import STOCK_TIMESERIES

   env = ENVIRONMENT()

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   def prep(*ini_list) :
       ini_list = filter(lambda x : "benchmark" in x , ini_list)
       print (ini_list)
       for path, section, key, stock_list in INI.loadList(*ini_list) :
           if section == 'Index' : pass
           else : continue
           yield key, stock_list

   file_list = env.list_filenames('local/historical_prices/*pkl')
   ini_list = env.list_filenames('local/*.ini')

   reader = STOCK_TIMESERIES.init()
   for name, stock_list in prep(*ini_list) :
       for stock in stock_list :
           print ((stock,name))
           data = reader.extract_from_yahoo(stock)
           if data is None : continue
           ret = data[['Adj Close']]
           print (ret.head(2))
           print (ret.tail(2))
           print (ret.mean())
           print (ret.std())
           print (ret.mean()[0])
           print (ret.std()[0])
           print (HELPER.find(ret,period=FINANCE.YEAR,span=0))
           print (HELPER.find(ret,period=FINANCE.YEAR))
           print ((stock,name))
