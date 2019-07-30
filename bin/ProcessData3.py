#!/usr/bin/python

import os,sys
import datetime
import ConfigParser
import pandas as pd
import numpy as np
from itertools import combinations as iter_combo

from ProcessData import CompareStock
from libCommon import INI, STOCK_TIMESERIES

def combinations(stock_list,size=5) :
    print stock_list
    ret_list = iter_combo(stock_list,size)
    for ret in list(ret_list):
        yield list(ret)

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
    spy_list = filter(lambda path : 'SPY' in path, file_list)
    control = spy_list[0]
    file_list = filter(lambda path : control != path, file_list)
    label, data = STOCK_TIMESERIES.load(control)
    return label, data, file_list

if __name__ == '__main__' :

   from glob import glob
   import os,sys
   from libMonteCarlo import MonteCarlo

   pwd = os.getcwd()
   pwd = pwd.replace('bin', 'local')
   ini_list = glob('{}/*.ini'.format(pwd))
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))

   name, spy_data, file_list = prep(*file_list)

   annual = MonteCarlo.YEAR()
   _ret, _dev, _sharpe, _length= annual.single(spy_data['Adj Close']) 
   print "{} return {}, dev {}, sharpe {} length {}".format(name,_ret,_dev,_sharpe, _length)
   print 

   perf_list = init(*ini_list)
   for name, data in main(file_list,perf_list) :
       ret, dev, sharpe, length = annual.single(data['Adj Close']) 
       flag_long = length > 250
       flag_stable = dev < 3*_dev
       flag_profit = ret > _ret
       flag_sharpe = sharpe > _sharpe
       if flag_long and flag_stable and flag_profit and flag_sharpe :
          print "{} return {}, dev {}, sharpe {}, length {}".format(name,ret,dev,sharpe,length)
       del data
