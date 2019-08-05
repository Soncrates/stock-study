#!/usr/bin/python

import pandas as pd
from libCommon import INI, STOCK_TIMESERIES, combinations
from libNasdaq import getByNasdaq
from libMonteCarlo import MonteCarlo

def main(file_list, ini_list) :
    Sector = {}
    Industry = {}
    Category = {}
    for path, section, key, stock in INI.loadList(*ini_list) :
        if 'nasdaq_top' not in path : continue
        if section == 'Sector' : config = Sector
        elif section == 'Industry' : config = Industry
        elif section == 'Fund' : config = Industry
        else : continue
        config[key] = stock
    for key, risky, balanced, safe in  _main(file_list, 'Fund', Category) :
        print "Fund {} risky : {} balanced : {} safe : {}".format(key,risky,balanced,safe)
    for key, risky, balanced, safe in  _main(file_list, 'Sector', Sector) :
        print "Sector {} risky : {} balanced : {} safe : {}".format(key,risky,balanced,safe)
    for key, risky, balanced, safe in  _main(file_list, 'Industry', Industry) :
        print "Industry {} risky : {} balanced : {} safe : {}".format(key,risky,balanced,safe)

def _main(file_list, name, kwargs) :
    ret_good = {}
    ret_ok = {}
    ret_bad = {}
    dev_good = {}
    dev_ok = {}
    dev_bad = {}
    for key in kwargs.keys() :
        for max_sharpe, min_dev in _process_01(file_list, kwargs[key], "{} {}".format(name, key)) :
            if max_sharpe['sharpe'] > 2 :
               if key not in ret_good.keys() : ret_good[key] = []
               ret_good[key].append(max_sharpe)
            elif max_sharpe['sharpe'] > 1 :
               if key not in ret_ok.keys() : ret_ok[key] = []
               ret_ok[key].append(max_sharpe)
            else : 
                if key not in ret_bad.keys() : ret_bad[key] = []
                ret_bad[key].append(max_sharpe)
            if min_dev['stdev'] < 0.25 :
               if key not in dev_good.keys() : dev_good[key] = []
               dev_good[key].append(min_dev)
            elif min_dev['stdev'] < 0.5 :
               if key not in dev_ok.keys() : dev_ok[key] = []
               dev_ok[key].append(min_dev)
            else : 
               if key not in dev_bad.keys() : dev_bad[key] = []
               dev_bad[key].append(min_dev)
    risky_data = {}
    for key, data in _process_02(**ret_good) :
        risky_data[key] = data
    safe_data = {}
    for key, data in _process_02(**dev_good) :
        safe_data[key] = data
    key_list = risky_data.keys() + safe_data.keys()
    key_list = list(set(key_list))
    for key in key_list :
        risky = set(risky_data.get(key,[]))
        safe = set(safe_data.get(key,[]))
        balanced = risky.intersection(safe)
        risky = risky - balanced
        safe = safe - balanced
        yield key, list(risky), list(balanced), list(safe)
def _process_02(**list_montecarlo) :
    meta = ['ret','stdev', 'sharpe']
    set_meta = set(meta)
    for key in list_montecarlo.keys() :
        ret = pd.DataFrame()
        for value in list_montecarlo[key] :
            p = pd.DataFrame(value).T
            ret = ret.append(p)
        size = int(len(ret)*.9)
        if size < 20 : size = 20
        ret = ret.sort_values(['stdev','sharpe']).head(size)
        ret = ret.fillna(0)
        mean = ret.mean()
        mean = pd.DataFrame(mean)
        mean.columns = ['mean']
        mean = mean[(mean['mean'] > 0.05)]
        mean = mean.T
        mean_intersection = set_meta.intersection(set(mean.columns))
        mean = mean.drop(columns=list(mean_intersection))
        yield key, list(mean.columns)
def _process_01(file_list, fund_performers, category) :
    annual = MonteCarlo.YEAR()
    nasdaq_name = []
    nasdaq_data = pd.DataFrame() 
    for name, stock in STOCK_TIMESERIES.read(file_list, fund_performers) :
        nasdaq_name.append(name)
        nasdaq_data[name] = stock['Adj Close']
    for subset in combinations(nasdaq_name,4) : 
        max_sharp, min_dev = annual(subset,nasdaq_data,5000) 
        yield max_sharp, min_dev

if __name__ == '__main__' :

   from glob import glob
   import os,sys

   pwd = os.getcwd()
   pwd = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(pwd))
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))

   main(file_list,ini_list)
   '''
   config = INI.init()
   INI.write_section(config,'Sector',**Sector_Top)
   INI.write_section(config,'Industry',**Industry_Top)
   INI.write_section(config,'Fund',**Fund_Top)
   stock_ini = "{}/nasdaq_top.ini".format(pwd)
   config.write(open(stock_ini, 'w'))
   '''
