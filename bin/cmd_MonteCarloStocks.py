#!/usr/bin/python

from libCommon import INI, STOCK_TIMESERIES, combinations
from libNasdaq import filterByNasdaq

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

def main(file_list, ini_list) :
    stock_list, fund_list, stock_performers, fund_performers = filterByNasdaq(*ini_list)

    name, spy_data = prep(*file_list) 

    annual = MonteCarlo.YEAR()

    stock_data = pd.DataFrame() 
    stock_data[name] = spy_data['Adj Close']

    max_sharp, min_vol = annual([name],stock_data,1200) 
    print max_sharp
    print min_vol

    stock_name = []
    for name, stock in read(file_list, stock_performers) :
        stock_name.append(name)
        stock_data[name] = stock['Adj Close']
    for subset in combinations(stock_name) : 
        print subset
        max_sharp, min_vol = annual(subset,stock_data,1200) 
        print max_sharp
        print min_vol

if __name__ == '__main__' :

   from glob import glob
   import os,sys
   import pandas as pd
   from libMonteCarlo import MonteCarlo

   pwd = os.getcwd()
   pwd = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(pwd))
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))
   main(file_list, ini_list)
