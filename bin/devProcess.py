#!/usr/bin/python
import pandas as pd
from pandas import DataFrame as df
import datetime
import math
import numpy as np

from libCommon import STOCK_TIMESERIES

'''
WARNING : not currently in use

Original idea : compare each stock to a benchmark (S&P or index fund)
                Partition stock prices by fiscal quarter (ten years worth)
                record all stocks that out performed the benchmark

The idea was to buy stocks that increas the most for a given quarter. then sell and buy new stock for the next quarter

This idea was abandoned when stocks were discovered that always do better than benchmark i.e.

from ini file :

Q1234 = AEE,AMZN,CMS,D,DHI,DHR,DOV,DTE,ED,EQR,HD,LHX,MA,MMC,MO,NKE,NOC,PYPL,QLD,SBUX,SPXL,SSO,STI,TQQQ,UNH,UNP,UPRO,WEC
      
'''
class CompareStock(object) :
  @staticmethod
  def init(**kwargs) :
      target = 'baseline_name'
      baseline_name = kwargs.get(target,'SPY')
      target = 'baseline_data'
      baseline_data = kwargs.get(target,{})
      target = 'contender_name'
      contender_name = kwargs.get(target,'AAA')
      target = 'contender_data'
      contender_data = kwargs.get(target,{})
      ret = CompareStock(baseline_name, baseline_data,contender_name,contender_data)
      #print str(ret)
      return ret
  def __init__(self,baseline_name, baseline_data,contender_name,contender_data) :
      self.spy_name = baseline_name
      self.contender_name = contender_name
      self.spy = baseline_data
      self.contender = contender_data
      self.spy2 = baseline_data.reindex(contender_data.index)
  def __str__(self) :
      key_list = self.__dict__.keys()
      value_list = map(lambda x : self.__dict__.get(x), key_list)
      key_list = map(lambda x : '{}.{}'.format(self.__class__.__name__,x),key_list)
      ret = dict(zip(key_list,value_list))
      return str(ret)
  def byAdjClose(self) :
      target = 'Adj Close'
      data = {self.spy_name: self.spy2[target], self.contender_name: self.contender[target]}
      data = pd.DataFrame(data=data)
      adjusted = data[self.contender_name]-data[self.spy_name]
      data[target] = adjusted
      return self.contender, data
  def byVolume(self) :
      target = 'Volume'
      data = {self.spy_name: self.spy2[target], self.contender_name: self.contender[target]}
      data = pd.DataFrame(data=data)
      adjusted = data[self.contender_name]-data[self.spy_name]
      data[target] = adjusted
      return self.contender, data

class StockTransform(object) :
      @staticmethod
      def by_quarter(data):
          year_to_year = {}
          for month, year, dataSeries in StockTransform.group_by_month(data) :
              quarter = 1
              if month in [4,5,6] : quarter = 2
              elif month in [7,8,9] : quarter = 3
              elif month in [10,11,12] : quarter = 4
              percent = dataSeries.pct_change()
              percent = percent.replace([np.inf, -np.inf], np.nan)
              percent = percent.dropna(how='all')
              percent = percent.fillna(0)
              #percent = percent + 1
              #percent = percent.cumprod()
              if len(percent) == 0 : continue
              if quarter not in year_to_year :
                 year_to_year[quarter] = pd.DataFrame(percent)
              else :
                 init = len(year_to_year[quarter])
                 year_to_year[quarter] = year_to_year[quarter].append(percent)
                 if init == len(year_to_year[quarter]) :
                    raise ValueError("dataframe did not append")
          return year_to_year
      @staticmethod
      def by_month(name, data):
          year_to_year = {}
          many_years = {}
          for month, year, dataSeries in StockTransform.group_by_month(data) :
              percent = dataSeries.pct_change()
              percent = percent.replace([np.inf, -np.inf], np.nan)
              percent = percent.dropna(how='all')
              #percent = percent + 1
              #percent = percent.cumprod()
              if len(percent) == 0 : continue
              if month not in year_to_year :
                 year_to_year[month] = pd.DataFrame(percent)
              else :
                 init = len(year_to_year[month])
                 year_to_year[month] = year_to_year[month].append(percent)
                 if init == len(year_to_year[month]) :
                    raise ValueError("dataframe did not append")
              label = '{},{}-{}'.format(month, year, name)
              many_years[label] = StockTransform.many_years(month, year, percent)
          return year_to_year, many_years 
      @staticmethod
      def group_by_month(data):
          ret = df.groupby(data,by=[data.index.month,data.index.year])
          for key in ret.groups :
              month, year = key[0],key[1]        
              yield month, year, ret.get_group(key)
      @staticmethod
      def many_years(month, year, data):
          lastday = StockTransform.getLastDay(month, year)
          days = range(1,lastday+1)
          ret =  dict.fromkeys(days,None)
          for x in data.index :
              ret[x.day]=data[data.index.day==x.day]
          return ret
      @staticmethod
      def getLastDay(month,year) :
          if month != 12 :
             month += 1
          else :
             year += 1
             month = 1
          ret = datetime.date (year, month, 1) - datetime.timedelta (days = 1)
          return ret.day

class Monthly_Transform(object) :
      @staticmethod
      def init(**kwargs) :
          target = 'spy_name'
          baseline_name = kwargs.get(target, "")
          target = 'contender_name'
          contender_name = kwargs.get(target, "")
          target = 'spy2'
          baseline = kwargs.get(target, {})
          target = 'contender'
          contender = kwargs.get(target, {})
          ret = Monthly_Transform(baseline_name, baseline, contender_name, contender)
          return ret
      def __init__(self, baseline_name, baseline, contender_name, contender) :
          self.baseline_name = baseline_name
          self.contender_name = contender_name
          self.baseline = baseline
          self.contender = contender
      def __str__(self) :
          key_list = self.__dict__.keys()
          value_list = map(lambda x : self.__dict__.get(x), key_list)
          key_list = map(lambda x : '{}.{}'.format(self.__class__.__name__,x),key_list)
          ret = dict(zip(key_list,value_list))
          return str(ret)
      def __call__(self, **kwargs) :
            
          #month_list=[11]
          #percent = percent.iloc[percent.index.month==month_list]
          #a,b = StockTransform.by_month(self.baseline_name,self.baseline)
          c,d = StockTransform.by_month(self.contender_name,self.contender)
          return c,d
      def gen_normalize_by_month(self, data) :
          for month, year, dataSeries in StockTransform.group_by_month(data) :
              ret = (dataSeries + 1).cumprod()
              ret['normalized']=(ret[self.contender]-ret[self.baseline]) + 1
              label = '{},{}-{}'.format(month, year,self.contender)
              data_2 = StockTransform.many_years(month, year,ret['normalized'])
              yield month, year, dataSeries, label, data_2 

class Partition(object) :
      @staticmethod
      def by_quarter(test):
          ret = {}
          for month, year, dataSeries in Partition.group_by_month(test) :
              if len(dataSeries) == 0 : continue
              dataSeries = Normalize._t01(dataSeries)
              if len(dataSeries) == 0 : continue
              quarter = 1
              if month in [4,5,6] : quarter = 2
              elif month in [7,8,9] : quarter = 3
              elif month in [10,11,12] : quarter = 4
              if quarter not in ret :
                 ret[quarter] = pd.DataFrame(dataSeries)
              else :
                 init = len(ret[quarter])
                 ret[quarter] = ret[quarter].append(dataSeries)
                 if init == len(ret[quarter]) :
                    raise ValueError("dataframe did not append")
          return ret
      @staticmethod
      def group_by_month(data):
          ret = df.groupby(data,by=[data.index.month,data.index.year])
          for key in ret.groups :
              month, year = key[0],key[1]
              yield month, year, ret.get_group(key)
      @staticmethod
      def sample_by_quarter(data):
          return data.resample('4M')

class FindStable(object) :
      @staticmethod
      def byVolume(test, threshold=1.5) :
          target = 'Volume'
          ret = test[target].std()
          return ret < threshold
      @staticmethod
      def byAdjClose(test, threshold=1.5) :
          target = 'Adj Close'
          ret = test[target].std()
          return ret < threshold

class Normalize(object) :
      @staticmethod
      def t01(baseline,test) :
          baseline = baseline.reindex(test.index)
          baseline = Normalize._t01(baseline)
          test = Normalize._t01(test)
          ret = test / baseline
          ret = ret.replace([np.inf, -np.inf], np.nan)
          ret = ret.fillna(0)
          ret = ret.dropna(how='all')
          return ret
      @staticmethod
      def _t01(df) :
          ret = df.pct_change()
          # clean up data
          ret = ret.replace([np.inf, -np.inf], np.nan)
          #ret = ret.fillna(0)
          ret = ret.dropna(how='all')
          return ret

def loadData(*file_list) :
    file_list = sorted(file_list)
    spy = filter(lambda stock : 'SPY' in stock, file_list)

    spy_filename = spy[0]
    spy_name, spy_data = STOCK_TIMESERIES.load(spy_filename)

    spy_quarterly = StockTransform.by_quarter(spy_data)
    key_list = spy_quarterly.keys()

    file_list = filter(lambda path : spy_filename not in path, file_list)
    for file_path in file_list :
        name, data = STOCK_TIMESERIES.load(file_path)
        quarterly = StockTransform.by_quarter(data)
        for key in key_list :
            if key not in quarterly : continue
            if len(quarterly[key]) == 0 : continue
            yield key, name, data, quarterly[key], spy_name, spy_data, spy_quarterly[key]

if __name__ == '__main__' :

   import os
   from glob import glob

   pwd = os.getcwd()
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))
   q = []
   name = None
   pair_list = []
   for key, contender_name, contender_data, contender_quarter, spy_name, spy_data, spy_quarter in loadData(*file_list) :
       if key == 1 :
           if isinstance(name,basestring) :
              print name, q, pair_list
           q = []
           pair_list = []
           name = None
       obj = CompareStock.init(baseline_name=spy_name,
                            baseline_data=spy_quarter,
                            contender_name=contender_name, 
                            contender_data=contender_quarter)
       name, vol = obj.byVolume()
       vol = pd.DataFrame(vol,columns=[spy_name,contender_name])
       std_list = {}
       for column in vol.columns :
           std = vol[column].values.std(ddof=1)
           std_list[column] = std
       del vol
       flag_stable = 1.5*std_list[spy_name] > std_list[contender_name]
       flag_isNum = not math.isnan(std_list[contender_name])
       if not flag_isNum :
          print contender_quarter
       if not flag_isNum or not flag_stable : 
          del obj
          del contender_quarter
          continue 
       std = std_list[contender_name]/std_list[spy_name]
       std = round(std*100,2)
       name, clo = obj.byAdjClose()
       clo = pd.DataFrame(clo,columns=['Adj Close'])
       mean = 0
       for column in clo.columns :
           mean = clo[column].values.mean()
           break
       del clo
       flag_better_than_spy = mean > 0 
       mean = round(mean*10000,2)
       if not flag_better_than_spy :
          del obj
          del contender_quarter
          continue
       q.append(key)
       name = contender_name
       pair_list.append((std,"{}e-4".format(mean)))
       del obj
       del contender_quarter
   print "Done"
