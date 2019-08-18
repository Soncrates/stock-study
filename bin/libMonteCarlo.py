import pandas as pd
import numpy as np
import warnings
import logging

class HELPER :
      key_list = ['returns', 'risk','sharpe','len']
      @staticmethod
      def find(data, **kwargs) :
          logging.debug(kwargs)
          if not isinstance(data,pd.DataFrame) :
             warnings.warn("prices are not in a dataframe {}".format(type(data)), RuntimeWarning)
             data = pd.DataFrame(data)
          target = "period"
          period = kwargs.get(target,0)
          target = "risk_free_rate"
          risk_free_rate = kwargs.get(target,0.02)
          target = "span`"
          span = kwargs.get(target,500)
          if period < 0 : 
             warnings.warn("period must be positive", RuntimeWarning)
          if span < 0 : 
             warnings.warn("span must be positive", RuntimeWarning)
          return HELPER._find(data, risk_free_rate, period, span)

      @staticmethod
      def _find(data, risk_free_rate, period, span) :
          returns, risk, _len = HELPER._findRiskAndReturn(data,period,span)
          sharpe = 0
          if risk != 0 :
             sharpe = ( returns - risk_free_rate ) / risk
          return dict(zip(HELPER.key_list, [returns, risk, sharpe,_len]))

      @staticmethod
      def _findRiskAndReturn(data,period,span) :
          ret = data.pct_change().dropna(how="all")
          _len = len(ret)
          if _len < period :
             return 0, 0, _len
          if span > 0: 
             #weigth recent history more heavily that older history
             returns = ret.ewm(span=span).mean().iloc[-1]
             logging.debug(returns)
          else :
             returns = ret.mean()
             logging.debug(returns)
          risk = ret.std()
          if isinstance(returns,pd.Series) : returns = returns[0]
          if isinstance(risk,pd.Series) : risk = risk[0]
          if period > 0 :
             returns *= period
             logging.debug(returns)
             risk *= np.sqrt(period)
             logging.debug(returns)
          return returns, risk, _len

      @staticmethod
      def dev_findWeightedSharpe(data, risk_free_rate=0.02) :
          if not isinstance(data,pd.DataFrame) :
             warnings.warn("prices are not in a dataframe", RuntimeWarning)
             data = pd.DataFrame(data)
          if period < 0 : 
             warnings.warn("period must be positive", RuntimeWarning)
          if span < 0 : 
             warnings.warn("span must be positive", RuntimeWarning)
          #select random weights for portfolio holdings
          weights = np.array(np.random.random(size))
          #rebalance weights to sum to 1
          weights /= np.sum(weights)
          return HELPER._dev_findWeightedSharpe(data, weights, risk_free_rate, period, span)

      @staticmethod
      def _dev_findWeightedSharpe(data, risk_free_rate=0.02, period=0, span=500) :
              # Linear Algebra magic
              magic = np.dot(cov_matrix, weights)
              magic_number = np.dot(weights.T,magic)

              #calculate return and volatility
              returns = np.sum(mean * weights) * self.period
              std_dev = np.sqrt(magic_number) * np.sqrt(self.period)

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
          stocks = self._filterBadSharpe(stocks,data)
          data = data[stocks]
          data.sort_index(inplace=True)
          #convert daily stock prices into daily returns
          returns = data.pct_change()
          portfolio_list = self._process(len(stocks), returns, num_portfolios)

          #convert results array to Pandas DataFrame
          columns = ['ret','stdev','sharpe']
          columns += stocks
          results_frame = pd.DataFrame(portfolio_list.T,columns=columns)
          #locate position of portfolio with highest Sharpe Ratio
          max_sharpe = results_frame['sharpe'].idxmax()
          max_sharpe_port = results_frame.iloc[max_sharpe]
          #locate positon of portfolio with minimum standard deviation
          min_vol = results_frame['stdev'].idxmin()
          min_vol_port = results_frame.iloc[min_vol]
          return max_sharpe_port, min_vol_port

      def _filterBadSharpe(self, stock_list, data) :
          ret = []
          for stock in stock_list :
              if stock not in data : continue
              d = data[stock]
              d = self.findSharpe(d)
              sharpe = d.get('sharpe',0)
              if  sharpe == 0 : continue
              ret.append(stock)
          return sorted(ret)

      def _process(self, size, returns, num_portfolios) :
          #set up array to hold results
          #We have increased the size of the array to hold the weight values for each stock
          ret = np.zeros((3+size,num_portfolios))

          #calculate mean daily return and covariance of daily returns
          mean = returns.mean()
          cov_matrix = returns.cov()
          for i in xrange(num_portfolios):
              #select random weights for portfolio holdings
              weights = np.array(np.random.random(size))
              #rebalance weights to sum to 1
              weights /= np.sum(weights)
              #logging.debug(weights)
              # Linear Algebra magic
              magic = np.dot(cov_matrix, weights)
              #logging.debug(magic)
              magic_number = np.dot(weights.T,magic)
              #logging.debug(magic_number)

              #calculate return and volatility
              returns = np.sum(mean * weights) * self.period
              std_dev = np.sqrt(magic_number) * np.sqrt(self.period)
              #calculate Sharpe Ratio (return / volatility) - risk free rate element excluded for simplicity
              sharpe = 0 
              if std_dev != 0 : sharpe = returns / std_dev
    
              #store results in results array
              ret[0,i] = returns
              ret[1,i] = std_dev
              ret[2,i] = sharpe
              for j in range(len(weights)):
                  ret[j+3,i] = weights[j]
          return ret

if __name__ == "__main__" :
   
   from glob import glob
   import os,sys,time

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

