#!/usr/bin/python

import datetime
import pandas as pd

import os,sys

pwd = os.getcwd()
pwd = pwd.replace('test','bin')
sys.path.append(pwd)

from libCommon import INI, STOCK_TIMERIES
from libQuantTrial_01 import DataFrame, DateFinder, CompoundAnnualGrowthRate

def prototype(path) :
    name, ret = STOCK_TIMESERIES.load(path)
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
        yield name, ret

def prep(*file_list) :
    file_list = sorted(file_list)
    spy_list = filter(lambda x : 'SPY' in x, file_list)
    spy = spy_list[0]
    name, ret = STOCK_TIMESERIES.load(spy)
    file_list = filter(lambda x : x != spy, file_list)
    return name, ret, spy, file_list

if __name__ == '__main__' :

   from glob import glob
   import os,sys

   pwd = os.getcwd()
   pwd = pwd.replace('test','local')
   ini_list = glob('{}/*.ini'.format(pwd))
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))
   stock_list = init(*ini_list)
   spy_name, spy_data, spy_path, file_list = prep(*file_list)

   name, spy = prototype(spy_path)
   print name
   for d in sorted(spy.keys()) :
       print d, spy[d]
   stock_name = []
   stock_data = pd.DataFrame() 
   for name, data in main(file_list, stock_list) :
       print '{} vs {}'.format(name, spy_name)
       a = getQuant(spy_data, data)
       print a
       stock_name.append(name)
       stock_data[name] = data['Adj Close']
       for d in sorted(data.keys()) :
           print d, round(data[d]/spy_data[d],2)
       del data
