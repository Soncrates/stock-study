import sys
import datetime
import logging
import numpy as np

if sys.version_info < (3, 0):
   import pandas as pd
   import pandas_datareader as web
   from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
else :
   '''
       import pandas_datareader as web
         ModuleNotFoundError: No module named 'pandas_datareader'
   '''
   import pandas as pd
   import pandas_datareader as web
   from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
   
'''
  NASDAQ - wrapper class around pandas built-in nasdaq reader
         , creates csv of all nasdaq stocks and funds
  TIME_SERIES - perhaps the only legit class in the entire library
              - defaults to pulling 10 years of stock data
              - Stock data saved as pkl files
'''

class NASDAQ :
      path = 'nasdaq.csv'
      def __init__(self, results) :
          self.results = results
      def __call__(self) :
          if self.results is None : return
          for name, alt_name, row in NASDAQ.extract(self.results) :
              stock = row.get(name)
              if not isinstance(stock,str) :
                 stock = row.get(alt_name)
              stock = NASDAQ.transform(stock)
              yield stock
      @classmethod
      def transform(cls, stock) :
          if '-' not in stock :
             return stock
          stock = stock.split('-')
          stock = '-P'.join(stock)
          return stock
      @classmethod
      def init(cls, **kwargs) :
          target = 'filename'
          filename = kwargs.get(target, None)
          target = 'retry_count'
          retry_count = kwargs.get(target,3)
          target = 'timeout'
          timeout = kwargs.get(target,30)

          results = get_nasdaq_symbols(retry_count, timeout)
          if filename is not None :
             results.to_csv(filename)
          ret = cls(results)
          return ret
      @classmethod
      def extract(cls, results) :
          symbol_list = filter(lambda x : 'Symbol' in x, results.columns)
          for index, row in results.iterrows():
              symbol_value = map(lambda x : row[x],symbol_list)
              ret = dict(zip(symbol_list,symbol_value))
              logging.info(ret)
              yield symbol_list[1], symbol_list[0], ret

class STOCK_TIMESERIES :
      @classmethod
      def init(cls, **kwargs) :
          target = 'end'
          end = kwargs.get(target, datetime.datetime.utcnow())
          target = 'start'
          start = kwargs.get(target, datetime.timedelta(days=365*10))
          start = end - start
          ret = cls(start,end)
          logging.debug(str(ret))
          return ret
      def __init__(self,start,end) :
          self.start = start
          self.end   = end
      def __str__(self) :
          ret = "{} to {}".format(self.start, self.end)
          return ret
      def extract_from_yahoo(self, stock) :
          ret = self._extract_from(stock, 'yahoo') 
          return ret
      def _extract_from(self, stock, service) :
          try :
             return web.DataReader(stock, service, self.start, self.end) 
          except Exception as e : logging.error(e, exc_info=True)

      @classmethod
      def save(cls, filename, stock, data) :
          if data is None : return
          data['Stock'] = stock
          data.to_pickle(filename)

      @classmethod
      def load(cls, filename) :
          data = pd.read_pickle(filename)
          target = 'Stock'
          if target in data :
             name = data.pop(target)
             name = name[0]
             return name, data
          name = filename.split("/")[-1]
          name = name.split(".")[0]
          return name, data
      @classmethod
      def read(cls, file_list, stock_list) :
          if stock_list is None or len(stock_list) == 0 :
             for path in file_list :
                 name, ret = STOCK_TIMESERIES.load(path)
                 yield name, ret
             return
              
          for path in file_list :
              flag_maybe = filter(lambda x : x in path, stock_list)
              flag_maybe = len(flag_maybe) > 0
              if not flag_maybe : continue
              name, ret = STOCK_TIMESERIES.load(path)
              if name not in stock_list :
                 del ret
                 continue
              yield name, ret
      @classmethod
      def read_all(cls, file_list, stock_list) :
          name_list = []
          data = None
          for stock_name, stock_data in STOCK_TIMESERIES.read(file_list, stock_list) :
             try :
               name_list.append(stock_name)
               stock_data.columns = pd.MultiIndex.from_product([[stock_name], stock_data.columns])
               if data is None:
                  data = stock_data
               else:
                  data = pd.concat([data, stock_data], axis=1)
             except Exception as e :  logging.error(e, exc_info=True)
             finally : pass
          return name_list, data
      @classmethod
      def flatten(cls, target,d) :
          d = d.iloc[:, d.columns.get_level_values(1)==target]
          d.columns = d.columns.droplevel(level=1)
          return d

class HELPER :
      YEAR = 252
      QUARTER = 63
      MONTH = 21
      WEEK = 5
      RESAMPLE_YEAR = '12M'
      RESAMPLE_QUARTER = '3M'
      RESAMPLE_MONTH = 'M'
      @classmethod
      def findDailyReturns(cls, data, period=0) :
          ret = data.pct_change()
          ret = ret.replace([np.inf, -np.inf], np.nan)
          height, width = ret.shape
          if width == 1 :
             ret = ret.dropna(how="all")
          #else :
          #   ret.fillna(0, inplace=True)
          height, width = ret.shape
          if height < period :
             return None
          ret.iloc[0] = 0  # set first day pseudo-price
          #ret.sort_index(inplace=True)
          logging.debug(ret.head(3))
          logging.debug(ret.tail(3))
          return ret
      @classmethod
      def graphReturns(cls, data) :
          #data = data.resample(HELPER.RESAMPLE_MONTH).ffill()
          ret = cls.findDailyReturns(data)
          ret = cls.transformReturns(ret)
          ret = ret.rolling(HELPER.MONTH).mean()
          logging.debug(ret.head(3))
          logging.debug(ret.tail(3))
          return ret
      @classmethod
      def transformReturns(cls, ret) :
          ret = 1 + ret
          ret = ret.cumprod()
          return ret
      @classmethod
      def findRiskAndReturn(cls, data, period=0, span=0) :
          returns, risk = cls._findRiskAndReturn(data, span)
          if isinstance(returns,pd.Series) : returns = returns[0]
          if isinstance(risk,pd.Series) : risk = risk[0]
          if period > 0 :
             returns *= period
             risk *= np.sqrt(period)
          return returns, risk
      @classmethod
      def _findRiskAndReturn(cls, data, span=0) :
          if span == 0 :
             returns = data.mean()
             risk = data.std()
             return returns, risk
          #weigth recent history more heavily that older history
          returns = data.ewm(span=span).mean().iloc[-1]
          risk = data.ewm(span=span).std().iloc[-1]
          return returns, risk

class RISK :
      column = 'risk'
      @classmethod
      def shave(cls, data, size) :
          ret = data.sort_values([cls.column]).head(size)
          logging.info(ret.sort_values([cls.column]).head(5))
          return ret
      @classmethod
      def trim(cls, data) :
          desc = data.describe()
          risk =  desc[cls.column]['75%']
          ret = data[data[cls.column] <= risk]
          logging.info(ret.sort_values([cls.column]).head(5))
          return ret
      @classmethod
      def cut(cls, data) :
          desc = data.describe()
          risk =  desc[cls.column]['25%']
          ret = data[data[cls.column] <= risk]
          logging.info(ret.sort_values([cls.column]).head(5))
          return ret

class RETURNS :
      column = 'returns'
      @classmethod
      def shave(cls, data, size) :
          ret = data.sort_values([cls.column]).tail(size)
          logging.info(ret.sort_values([cls.column]).tail(5))
          return ret
      @classmethod
      def trim(cls, data) :
          desc = data.describe()
          returns =  desc[cls.column]['25%']
          ret = data[data[cls.column] >= returns]
          logging.info(ret.sort_values([cls.column]).tail(5))
          return ret
      @classmethod
      def cut(cls, data) :
          desc = data.describe()
          returns =  desc[cls.column]['75%']
          ret = data[data[cls.column] >= returns]
          logging.info(ret.sort_values([cls.column]).tail(5))
          return ret

class BIN :
      @classmethod
      def descending(cls, data,target) :
          desc = data.describe()
          logging.debug(desc)
          _bin1 =  desc[target]['75%']
          _bin2 =  desc[target]['50%']
          _bin3 =  desc[target]['25%']
          logging.debug((_bin1,_bin2,_bin3))
          bin1 = data[data[target] > _bin1]
          bin2 = data[(data[target] <= _bin1) & (data[target] > _bin2)]
          bin3 = data[(data[target] <= _bin2) & (data[target] > _bin3)]
          bin4 = data[data[target] <= _bin3]
          ret = [ bin1, bin2, bin3, bin4 ]
          ret = filter(lambda x : len(x) > 0, ret)
          return ret

      @classmethod
      def ascending(cls, data,target) :
          desc = data.describe()
          logging.debug(desc)
          _bin1 =  desc[target]['75%']
          _bin2 =  desc[target]['50%']
          _bin3 =  desc[target]['25%']
          logging.debug((_bin1,_bin2,_bin3))
          bin4 = data[data[target] < _bin3]
          bin3 = data[(data[target] >= _bin3) & (data[target] < _bin2)]
          bin2 = data[(data[target] >= _bin2) & (data[target] < _bin1)]
          bin1 = data[data[target] >= _bin1]
          ret = [ bin4, bin3, bin2, bin1 ]
          ret = filter(lambda x : len(x) > 0, ret)
          return ret

if __name__ == "__main__" :

   import sys
   import logging
   from libCommon import ENVIRONMENT, INI

   env = ENVIRONMENT()

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   file_list = env.list_filenames('local/historical_prices/*pkl')
   ini_list = env.list_filenames('local/*.ini')
   _nasdaq = env.list_filenames('local/'+NASDAQ.path)[0]
   def demo_nasdaq() :
       nasdaq = NASDAQ.init(filename=_nasdaq)
       for stock in nasdaq() :
           logging.debug(stock)
           if stock == 'AAPL' : break

   def prep() :
       for path, section, key, value in INI.loadList(*ini_list) :
           if 'Industry' not in section : continue
           if 'Gas' not in key : continue
           if 'Util' not in key : continue
           break
       return value[:2]

   def demo_stock() :
       reader = STOCK_TIMESERIES.init()
       for stock in stock_list :
           ret = reader.extract_from_yahoo(stock)
           logging.debug (stock)
           logging.debug (ret.describe())
           returns = HELPER.findDailyReturns(ret)
           logging.debug (returns.describe())
           logging.debug (HELPER.findRiskAndReturn(returns))
           logging.debug (HELPER.findRiskAndReturn(returns, span=HELPER.YEAR))

   def demo_stock_2() :
       a,b = STOCK_TIMESERIES.read_all(file_list, stock_list)
       b = STOCK_TIMESERIES.flatten('Adj Close',b)
       print (b.describe())
       print (a)
   stock_list = prep()
   demo_stock()
   #demo_stock2()
   demo_nasdaq()
