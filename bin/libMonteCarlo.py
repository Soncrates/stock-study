import pandas as pd
import numpy as np

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
          ret = MonteCarlo(15) 
          return ret
      def __init__(self, period) :
          self.period = period
      def findSharpe(self, data) :
          data.sort_index(inplace=True)
          if len(data) < self.period :
             return 0, 0, 0, len(data)
          returns = data.pct_change()
          mean = returns.mean()
          dev = returns.std()
          weighted_return = round(mean * self.period,2)
          weighted_dev = round(dev * np.sqrt(self.period),2)
          sharpe = 0
          if weighted_dev > 0 :
             sharpe = round(weighted_return/weighted_dev,2)
          return weighted_return, weighted_dev, sharpe, len(data)
      def __call__(self, stocks,data, num_portfolios = 25000) :
          data = data[stocks]
          data.sort_index(inplace=True)
          #convert daily stock prices into daily returns
          returns = data.pct_change()
          results = self._process(len(stocks), returns, num_portfolios)

          #convert results array to Pandas DataFrame
          columns = ['ret','stdev','sharpe']
          columns += stocks
          results_frame = pd.DataFrame(results.T,columns=columns)
          #locate position of portfolio with highest Sharpe Ratio
          max_sharpe_port = results_frame.iloc[results_frame['sharpe'].idxmax()]
          #locate positon of portfolio with minimum standard deviation
          min_vol_port = results_frame.iloc[results_frame['stdev'].idxmin()]
          return max_sharpe_port, min_vol_port

      def _process(self, size, returns, num_portfolios) :
          #set up array to hold results
          #We have increased the size of the array to hold the weight values for each stock
          results = np.zeros((3+size,num_portfolios))

          #calculate mean daily return and covariance of daily returns
          mean = returns.mean()
          cov_matrix = returns.cov()
          for i in xrange(num_portfolios):
              #select random weights for portfolio holdings
              weights = np.array(np.random.random(size))
              #rebalance weights to sum to 1
              weights /= np.sum(weights)
    
              #calculate portfolio return and volatility
              portfolio_return = np.sum(mean * weights) * self.period
              portfolio_std_dev = np.sqrt(np.dot(weights.T,np.dot(cov_matrix, weights))) * np.sqrt(self.period)
    
              #store results in results array
              results[0,i] = portfolio_return
              results[1,i] = portfolio_std_dev
              #store Sharpe Ratio (return / volatility) - risk free rate element excluded for simplicity
              results[2,i] = results[0,i] / results[1,i]
              #iterate through the weight vector and add data to results array
              for j in range(len(weights)):
                  results[j+3,i] = weights[j]
          return results

if __name__ == "__main__" :
   
   from libCommon import STOCK_TIMESERIES

   target = 'Adj Close'
   stock_list = ['AMZN','CMS','DOV','DTE','EQR','HD','LHX','MA','MMC','NKE','NOC','PYPL','QLD','SBUX','UNH','UNP','WEC']
   stock_list = stock_list[2::3]

   reader = STOCK_TIMESERIES.init()
   annual = MonteCarlo.YEAR()

   stock_data = pd.DataFrame()
   for stock in stock_list :
       data = reader.extract_from_yahoo(stock)
       stock_data[stock] = data[target]

       ret, dev, sharpe, length = annual.findSharpe(data[target])
       print "{} return {}, dev {}, sharpe {}, length {}".format(stock,ret,dev,sharpe,length)

   max_sharp, min_vol = annual(stock_list,stock_data)
   print max_sharp
   print min_vol

