#!/usr/bin/python

import pandas as pd
from libCommon import INI, STOCK_TIMESERIES, combinations
from libNasdaq import getByNasdaq
from libMonteCarlo import MonteCarlo

def read(file_list, stock_list) :
    for path in file_list :
        flag_maybe = filter(lambda x : x in path, stock_list)
        flag_maybe = len(flag_maybe) > 0
        if not flag_maybe : continue
        name, ret = STOCK_TIMESERIES.load(path)
        if name not in stock_list : 
           del ret      
           continue
        yield name, ret

def prep(*file_list) :
   file_list = sorted(file_list)
   spy_list = filter(lambda x : 'SPY' in x, file_list)
   spy = spy_list[0]
   name, data = STOCK_TIMESERIES.load(spy)
   return name, data

def main(file_list, fund_performers, category) :
    annual = MonteCarlo.YEAR()
    nasdaq_name = []
    nasdaq_data = pd.DataFrame() 
    for name, stock in read(file_list, fund_performers) :
        nasdaq_name.append(name)
        nasdaq_data[name] = stock['Adj Close']
    for subset in combinations(nasdaq_name,4) : 
        print category, sorted(subset)
        max_sharp, min_vol = annual(subset,nasdaq_data,5000) 
        print max_sharp
        print min_vol

def find(file_list, ini_list) :

    Sector_Top = {}
    Sector_Low = {}
    Industry_Top = {}
    Industry_Low = {}
    Fund_Top = {}
    Fund_Low = {}
    for section, key, top, top_data, low, low_data in findOptimal(file_list, ini_list) :
        if section == "Sector" :
           config_top = Sector_Top
           config_low = Sector_Low
        elif section == "Industry" :
           config_top = Industry_Top
           config_low = Industry_Low
        elif section == "Fund" :
           config_top = Fund_Top
           config_low = Fund_Low
        else : continue
        print section, key, sorted(top)
        print top_data
        print low_data
        config_top[key] = sorted(top)
        config_low[key] = sorted(low)
    return Sector_Top, Industry_Top, Fund_Top, Sector_Low, Industry_Low, Fund_Low

def findOptimal(file_list, ini_list) :
    Sector, Industry, Category, FundFamily = getByNasdaq(*ini_list)
    for key, top_columns, top_data, low_columns, low_data in _findOptimal(file_list, **Sector) :
        yield "Sector", key, top_columns, top_data, low_columns, low_data
    for key, top_columns, top_data, low_columns, low_data in _findOptimal(file_list, **Industry) :
        yield "Industry", key, top_columns, top_data, low_columns, low_data
    for key, top_columns, top_data, low_columns, low_data in _findOptimal(file_list, **Category) :
        yield "Fund", key, top_columns, top_data, low_columns, low_data

def _findOptimal(file_list, **kwargs) :
    annual = MonteCarlo.YEAR()
    for key in sorted(kwargs.keys()) :
        value_list = sorted(kwargs[key])
        fund_name = []
        fund_data = {}
        for name, data in read(file_list, value_list) :
            #ret, vol, sharpe, length = annual.single(data['Volume']) 
            ret, dev, sharpe, length = annual.single(data['Adj Close']) 
            #fund_data[name] = {'returns' : ret, 'dev' : vol, 'sharpe' : sharpe, 'length' : length}
            fund_data[name] = {'returns' : ret, 'dev' : dev, 'sharpe' : sharpe, 'length' : length}

        if len(fund_data) == 0 : continue
        ret = pd.DataFrame(fund_data).T
        # filter low performers
        ret = ret[(ret['returns'] > 0.1)]
        if len(ret) == 0 : continue
        # filter riskier
        size = int(len(ret)*.9)
        if size < 8 : size == 8
        ret = ret.sort_values(['dev']).head(size)
        if len(ret) == 0 : continue
        # screen top players
        top = ret.sort_values(['sharpe','returns']).tail(8)
        if len(top) == 0 : continue
        low = ret.sort_values(['sharpe','returns']).head(8)
        yield key, list(top.T.columns), top, list(low.T.columns), low

if __name__ == '__main__' :

   from glob import glob
   import os,sys

   pwd = os.getcwd()
   pwd = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(pwd))
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))
   Sector_Top, Industry_Top, Fund_Top, Sector_Low, Industry_Low, Fund_Low = find(file_list,ini_list)
   config = INI.init()
   INI.write_section(config,'Sector',**Sector_Top)
   INI.write_section(config,'Industry',**Industry_Top)
   INI.write_section(config,'Fund',**Fund_Top)
   stock_ini = "{}/nasdaq_top.ini".format(pwd)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   INI.write_section(config,'Sector',**Sector_Low)
   INI.write_section(config,'Industry',**Industry_Low)
   INI.write_section(config,'Fund',**Fund_Low)
   stock_ini = "{}/nasdaq_low.ini".format(pwd)
   config.write(open(stock_ini, 'w'))

