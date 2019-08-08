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
          ret = MonteCarlo(21) 
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
          weighted_return = mean * self.period
          weighted_dev = dev * np.sqrt(self.period)
          sharpe = 0
          if weighted_dev != 0 :
             sharpe = weighted_return/weighted_dev
          return weighted_return, weighted_dev, sharpe, len(data)
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
              returns, dev, sharpe, length = self.findSharpe(d)
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
              # Linear Algebra magic
              magic = np.dot(cov_matrix, weights)
              magic_number = np.dot(weights.T,magic)

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
   
   from libCommon import STOCK_TIMESERIES

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

       ret, dev, sharpe, length = annual.findSharpe(data[target])
       ret = round(ret,2)
       dev = round(dev,2)
       sharpe = round(sharpe,2)
       print "{} return {}, dev {}, sharpe {}, length {}".format(stock,ret,dev,sharpe,length)
       if sharpe == 0 : 
          continue
       stock_data[stock] = data[target]

   max_sharpe, min_vol = annual(stock_list,stock_data)
   max_sharpe = max_sharpe.round(2)
   min_vol = min_vol.round(2)
   print max_sharpe
   print min_vol

