import sys
import datetime
import logging
import numpy as np

'''
  TIME_SERIES - perhaps the only legit class in the entire library
              - defaults to pulling 10 years of stock data
              - Stock data saved as pkl files

Metric 
Start Balance	$10,000	$10,000
End Balance	$1,772,527	$8,567,187
CAGR	16.49%	22.03%
Expected Return	21.52%	34.34%
Stdev	29.28%	43.73%
Best Year	146.24%	211.90%
Worst Year	-47.62%	-71.06%
Max. Drawdown	-63.41%	-79.68%
Sharpe Ratio (ex-ante)	0.63	0.71
Sharpe Ratio (ex-post)	0.56	0.61
Sortino Ratio	0.89	0.98
US Stock Market Correlation	0.60	0.47

Annual Growth (completed)
Annual Return (bar chart by portfolio, grouped by year)


Arithmetic Mean (monthly)	1.64%	2.49%
Arithmetic Mean (annualized)	21.52%	34.34%
Geometric Mean (monthly)	1.28%	1.67%
Geometric Mean (annualized)	16.49%	22.03%
Volatility (monthly)	8.45%	12.63%
Volatility (annualized)	29.28%	43.73%
Downside Deviation (monthly)	5.23%	7.76%
Max. Drawdown	-63.41%	-79.68%
US Market Correlation	0.60	0.47
Beta(*)	1.16	1.37
Alpha (annualized)	6.77%	14.67%
R2	35.50%	22.19%
Sharpe Ratio	0.56	0.61
Sortino Ratio	0.89	0.98
Treynor Ratio (%)	14.22	19.49
Calmar Ratio	0.67	1.19
Active Return	6.09%	11.63%
Tracking Error	23.64%	38.98%
Information Ratio	0.26	0.30
Skewness	-0.08	-0.17
Excess Kurtosis	1.63	1.52
Historical Value-at-Risk (5%)	-12.36%	-17.43%
Analytical Value-at-Risk (5%)	-12.25%	-18.28%
Conditional Value-at-Risk (5%)	-16.89%	-26.07%
Upside Capture Ratio (%)	129.76	170.46
Downside Capture Ratio (%)	105.67	125.63
Safe Withdrawal Rate	9.63%	12.24%
Perpetual Withdrawal Rate	12.27%	16.36%
Positive Periods	247 out of 407 (60.69%)	231 out of 407 (56.76%)
Gain/Loss Ratio	1.08	1.28

#	Asset	                        CAGR	Expected Return*	Standard Deviation	Sharpe Ratio*	Min. Weight	Max. Weight
1	Apple Inc. (AAPL)	        22.03%	34.34%	                43.73%	                0.713	        0.00%	100.00%
2	Intern Bus Machines Corp (IBM)	6.15%	9.84%	                26.32%	                0.253	        0.00%	100.00%


'''

class STOCK_TIMESERIES :
      @classmethod
      def init(cls, **kwargs) :
          target = 'end'
          end = kwargs.get(target, datetime.datetime.utcnow())
          if isinstance(end, str) :
             end = datetime.datetime.strptime(end, '%Y-%m-%d')
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
          if not isinstance(stock_list,list) :
             stock_list = list(stock_list)
          if stock_list is None or len(stock_list) == 0 :
             for path in file_list :
                 name, ret = STOCK_TIMESERIES.load(path)
                 yield name, ret
             return
              
          for path in file_list :
              flag_maybe = filter(lambda x : x in path, stock_list)
              if not isinstance(flag_maybe,list) :
                 flag_maybe = list(flag_maybe)
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
          #ret = data / data.iloc[0]
          ret = data.pct_change()
          ret.iloc[0] = 0  # set first day pseudo-price
          ret = ret.replace([np.inf, -np.inf], np.nan)
          height, width = ret.shape
          if width == 1 :
             ret = ret.dropna(how="all")
          #else :
          #   ret.fillna(0, inplace=True)
          height, width = ret.shape
          if height < period :
             return None
          #ret.sort_index(inplace=True)
          #ret = 1 + ret
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
          risk, returns = cls._findRiskAndReturn(data, span)
          if isinstance(returns,pd.Series) : returns = returns[0]
          if isinstance(risk,pd.Series) : risk = risk[0]
          if period > 0 :
             returns *= period
             risk *= np.sqrt(period)
          return risk, returns
      @classmethod
      def _findRiskAndReturn(cls, data, span=0) :
          if span == 0 :
             returns = data.mean()
             risk = data.std()
             return risk, returns
          #weigth recent history more heavily that older history
          returns = data.ewm(span=span).mean().iloc[-1]
          risk = data.ewm(span=span).std().iloc[-1]
          return risk, returns
      @classmethod
      def CAGR(cls, data):
          periods = len(data) / float(cls.YEAR)
          first = data.head(1)[0]
          last = data.tail(1)[0]
          return cls._CAGR(first, last, periods)
      @classmethod
      def _CAGR(cls, first, last, periods):
          return (last/first)**(1/periods)-1

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

'''
#	Asset	                        CAGR	Expected Return*	Standard Deviation	Sharpe Ratio*	Min. Weight	Max. Weight
1	Apple Inc. (AAPL)	        22.03%	34.34%	                43.73%	                0.713	        0.00%	100.00%
2	Intern Bus Machines Corp (IBM)	6.15%	9.84%	                26.32%	                0.253	        0.00%	100.00%
'''

if __name__ == "__main__" :

   import sys
   import logging
   from libCommon import ENVIRONMENT, INI

   env = ENVIRONMENT()

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   file_list = env.list_filenames('local/historical_prices/*pkl')
   ini_list = env.list_filenames('local/*.ini')
   ini_list = filter(lambda x : 'background' in x, ini_list)
   ini_list = filter(lambda x : 'stock_' in x, ini_list)

   def prep(*ini_list) :
       for path, section, key, value in INI.loadList(*ini_list) :
           if 'Industry' not in section : continue
           if 'Gas' not in key : continue
           if 'Util' not in key : continue
           break
       return value[:2]

   def demo_stock() :
       end = "2019-06-01"
       reader = STOCK_TIMESERIES.init(end=end)
       for stock in stock_list :
           ret = reader.extract_from_yahoo(stock)
           logging.debug (stock)
           logging.debug (ret.describe())
           returns = HELPER.findDailyReturns(ret)
           logging.debug (returns.describe())
           _risk, _returns = HELPER.findRiskAndReturn(returns['Adj Close'])
           msg = [_returns,_risk,_returns/_risk]
           msg = zip(['returns','risk','sharpe'],msg)
           logging.debug(msg)
           _risk, _returns = HELPER.findRiskAndReturn(returns['Adj Close'], period=HELPER.YEAR )
           msg = [_returns,_risk,_returns/_risk]
           msg = zip(['returns','risk','sharpe'],msg)
           logging.debug(msg)
           logging.debug (HELPER.CAGR(ret['Adj Close']))

   def demo_stock_2() :
       a,b = STOCK_TIMESERIES.read_all(file_list, stock_list)
       b = STOCK_TIMESERIES.flatten('Adj Close',b)
       print (b.describe())
       print (a)
   #stock_list = prep(*ini_list)
   stock_list = ['AAPL','IBM']
   stock_list = ['IBM','AAPL','^GSPC']
   demo_stock()
   #demo_stock2()
   #demo_nasdaq()
