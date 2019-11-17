#!/usr/bin/python

import os,sys
import datetime
import ConfigParser
import pandas as pd
import numpy as np

from libCommon import INI
from libFinance import STOCK_TIMERIES, HELPER as FINANCE

'''
    WARNING : Not currently used

    Advanced financial data topics that were researched and ultimately put on the back burner.
    Also advance pandas dataframe use

'''
def loadData(*file_list) :
    file_list = sorted(file_list)
    spy = filter(lambda stock : 'SPY' in stock, file_list)

    spy_filename = spy[0]
    spy_name, spy_data = STOCK_TIMESERIES.load(spy_filename)

    file_list = filter(lambda path : spy_filename not in path, file_list)
    for file_path in file_list :
        name, data = STOCK_TIMESERIES.load(file_path)
        yield name, data, spy_name, spy_data

class DateFinder(object): 
    key_list = ["Years 10", "Years 5", "Year 1", "Months 6", "Months 3", "now"]
    value_list = [365*10, 365*5, 365, 180, 90, 30, 0]
    @classmethod
    def getDates(cls, **kwargs) :
        target = 'now'
        end = kwargs.get(target, datetime.datetime.utcnow())
        value_list = map(lambda d : end - datetime.timedelta(days=d), cls.value_list)
        ret = dict(zip(cls.key_list,value_list))
        return ret
    @classmethod
    def getRange(cls, arg) :
        day_list=range(arg.day-2,arg.day+2)
        day_list = filter(lambda x : x>0 and x<31, day_list)
        start = datetime.date(year=arg.year,month=arg.month,day=day_list[0])
        end = datetime.date(year=arg.year,month=arg.month,day=day_list[-1])
        return start, end

class DataFrameDateTime(object) :
    @classmethod
    def _get(cls, df, arg) :
        ret = df[df.index.year == arg.year]
        ret = ret[ret.index.month == arg.month]
        ret = ret.head(n=1)
        return ret

    @classmethod
    def get(cls, df, date_arg) :
        start, end = DateFinder.getRange(date_arg)
        ret = df.loc[start:end]
        ret = ret.tail(n=1)
        if len(ret) == 0 :
           ret = cls._get(df, date_arg)
        return ret

class DataFrameDateTime(object) :
    @classmethod
    def _get(cls, df, arg) :
        ret = df[df.index.year == arg.year]
        ret = ret[ret.index.month == arg.month]
        ret = ret.head(n=1)
        return ret

    @classmethod
    def get(cls, df, date_arg) :
        start, end = DateFinder.getRange(date_arg)
        ret = df.loc[start:]
        if len(ret) == 0 :
           ret = cls._get(df, date_arg)
        return ret

class DataFrame(object) :
    @classmethod
    def getLastDateTime(cls, data) :
        ret = data.tail(n=1)
        ret = ret.index
        ret = pd.to_datetime(ret)
        ret = ret.date
        if isinstance(ret,np.ndarray) :
           ret = ret[0] 
        return ret
    @classmethod
    def getFirstDateTime(cls, data) :
        ret = data.head(n=1)
        ret = ret.index
        ret = pd.to_datetime(ret)
        ret = ret.date
        if isinstance(ret,np.ndarray) :
           ret = ret[0] 
        return ret
class xxx(object) :
    @classmethod
    def get(cls, df) :
        ret = df.apply(lambda x: x / x[0])
        ret = ret - 1
        return ret

class Signals :
    @classmethod
    def get(cls, df) :
        short_window = 40
        long_window = 100
        return cls._get(df,short_window,long_window)

    @classmethod
    def _get(cls, df,short_window,long_window) :
        _short = 'short_mavg'
        _long = 'long_mavg'
        target = 'Close'

        # Initialize the `signals` DataFrame with the `signal` column
        ret = pd.DataFrame(index=df.index)
        ret['signal'] = 0.0

        # Create short simple moving average over the short window
        ret[_short]= df[target].rolling(window=short_window, min_periods=1, center=False).mean()

        # Create long simple moving average over the long window
        ret[_long] = df[target].rolling(window=long_window, min_periods=1, center=False).mean()

        # Create signals
        ret['signal'][short_window:] = np.where(ret[_short][short_window:]
                                            > ret[_long][short_window:], 1.0, 0.0)

        # Generate trading orders
        ret['positions'] = ret['signal'].diff()
        return ret

class Portfolio :
    @classmethod
    def get(cls, name, df, signals) :
        # Set the initial capital
        initial_capital= float(100000.0)
        shares = 100
        positions = cls.init(name, shares, signals)
        portfolio = cls._get(df,positions,initial_capital)

        # Isolate the returns of your strategy
        returns = portfolio['returns']

        # annualized Sharpe ratio
        sharpe_ratio = np.sqrt(252) * (returns.mean() / returns.std())
        return portfolio, returns, sharpe_ratio

    @classmethod
    def init(cls, name, shares, signals) :
        ret = pd.DataFrame(index=signals.index).fillna(0.0)
        ret[name] = shares*signals['signal']
        return ret

    @classmethod
    def _get(cls, df,positions,initial_capital) :
        target = 'Adj Close'

        # Initialize the portfolio with value owned
        ret = positions.multiply(df[target], axis=0)
        # Store the difference in shares owned
        pos_diff = positions.diff()

        ret['holdings'] = (positions.multiply(df[target], axis=0)).sum(axis=1)
        ret['cash'] = initial_capital - (pos_diff.multiply(df[target], axis=0)).sum(axis=1).cumsum()
        ret['total'] = ret['cash'] + ret['holdings']
        ret['returns'] = FINANCE.getDailyReturns(ret['total'])
        return ret

class CompoundAnnualGrowthRate :
    @classmethod
    def get(cls, df) :
        if len(df) < 2 :
            return 0
        target = 'Adj Close'
        ret = df[target]
        year = 365.0
        periods = (ret.index[-1] - ret.index[0]).days
        if periods > 0 :
           periods = year/periods
        else :
           periods = 1
        ret = ret[-1] / ret[1]
        ret = ( ret ** periods) - 1
        return ret

class ReturnDeviation(object) :
    @classmethod
    def get(cls, df) :
        target = 'Adj Close'
        ret = df[target]
        ret = ret / ret.shift(1) - 1
        return ret

def getFirstDay(data) :
    ret = data.head(n=1)
    if isinstance(ret,np.ndarray) :
       ret = ret[0] 
    return ret
def getLastDay(data) :
    ret = data.tail(n=1)
    if isinstance(ret,np.ndarray) :
       ret = ret[0] 
    return ret
def transformByFirstDay(trans,data) :
    temp = data.head(n=1)
    if isinstance(temp,np.ndarray) :
       temp = temp[0] 
    data = data.div(temp.iloc[0])
    return data

def getQuant(spy,data) :
    end = DataFrame.getLastDateTime(spy)
    dates = DateFinder.getDates(now=end)
    a = map(lambda d : DataFrameDateTime.get(spy,d), sorted(dates.values()))
    a = reduce(lambda c,d : c.append(d), a)
    b = map(lambda d : DataFrameDateTime.get(data,d), sorted(dates.values()))
    b = reduce(lambda c,d : c.append(d), b)
    temp = a.head(n=1)
    a = transformByFirstDay(temp,a)
    temp = b.head(n=1)
    b = transformByFirstDay(temp,b)
    return b / a

def prototype(_file) :
    name, ret = STOCK_TIMESERIES.load(_file)
    end = DataFrame.getLastDateTime(ret)
    dates = DateFinder.getDates(now=end)
    value_list = sorted(dates.values())
    ret = map(lambda d : DataFrameDateTime.get(ret,d), value_list)
    ret = map(lambda d : CompoundAnnualGrowthRate.get(d), ret)
    #ret = map(lambda d : xxx.get(d), ret)
    ret = dict(zip(value_list,ret))
    return name, ret

def init(*ini_list) :
    performers = {}
    stability = {}
    for file_name, name, key, value in INI.loadList(*ini_list) :
        config = None
        if name == "Stability" :
           config = stability
        if name == "Performance" :
           config = performers
        config[key] = value
    ret = performers.get('Q1234',[])
    return ret

def main(file_list, stock_list) :
    for path in file_list :
        name, ret = STOCK_TIMESERIES.load(path)
        if name not in stock_list : 
           del ret      
           continue
        '''
        if name in ['DTE','HD','LHX']: 
           del ret      
           continue
        if name in ['DHI','SPXL','TQQQ','UPRO']: 
           del ret      
           continue
        if name in ['QLD','SSO','STI','UNP', 'OV']: 
           del ret      
           continue
        if name in ['SBUX','AEE','DHR','DOV', 'EQR']: 
           del ret      
           continue
        '''
        yield name, ret

if __name__ == '__main__' :

   from glob import glob
   import os,sys

   pwd = os.getcwd()
   ini_list = glob('{}/*.ini'.format(pwd))
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))
   file_list = sorted(file_list)
   spy_list = filter(lambda x : 'SPY' in x, file_list)
   spy = spy_list[0]

   '''
   for name, data, spy_name, spy_data in loadData(*file_list) :
       if name not in stock_list : 
          del data
          del spy_data
          continue
       print '{} vs {}'.format(name, spy_name)
       a = getQuant(spy_data, data)
       #print a
       del data
       del spy_data
       break
   '''
   stock_list = init(*ini_list)
   name, spy = prototype(spy)
   print name
   for d in sorted(spy.keys()) :
       print d, spy[d]
   stock_name = []
   stock_data = pd.DataFrame() 
   for name, stock in main(file_list, stock_list) :
       stock_name.append(name)
       stock_data[name] = stock['Adj Close']
       '''
       for d in sorted(stock.keys()) :
           print d, round(stock[d]/spy[d],2)
       del stock
       '''
