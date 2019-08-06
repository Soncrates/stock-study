#!/usr/bin/python

import pandas as pd
from libCommon import INI, STOCK_TIMESERIES, combinations
from libNasdaq import getByNasdaq
from libMonteCarlo import MonteCarlo

def prep(*ini_list) :
    Sector = {}
    Industry = {}
    Category = {}
    for path, section, key, stock in INI.loadList(*ini_list) :
        if 'nasdaq_sharpe_top' not in path : continue
        if section == 'Sector' : config = Sector
        elif section == 'Industry' : config = Industry
        elif section == 'Fund' : config = Category
        else : continue
        config[key] = stock
    return Sector, Industry, Category

def main(file_list, ini_list) :
    Sector, Industry, Category = prep(*ini_list)

    ret_good, ret_ok, ret_bad, dev_good, dev_ok, dev_bad = _bin_by_MonteCarlo(file_list, Category)
    ret_cat, dev_cat = thing(ret_good, ret_ok, ret_bad, dev_good, dev_ok, dev_bad)
    ret_good, ret_ok, ret_bad, dev_good, dev_ok, dev_bad = _bin_by_MonteCarlo(file_list, Sector)
    ret_sect, dev_sect = thing(ret_good, ret_ok, ret_bad, dev_good, dev_ok, dev_bad)
    ret_good, ret_ok, ret_bad, dev_good, dev_ok, dev_bad = _bin_by_MonteCarlo(file_list, Industry)
    ret_ind, dev_ind = thing(ret_good, ret_ok, ret_bad, dev_good, dev_ok, dev_bad)
    
    for key, risky, balanced, safe in  sortMonteCarlo(ret_cat, dev_cat) :
        print "Fund {} risky : {} balanced : {} safe : {}".format(key,risky,balanced,safe)
    for key, risky, balanced, safe in  sortMonteCarlo(ret_sect, dev_sect) :
        print "Sector {} risky : {} balanced : {} safe : {}".format(key,risky,balanced,safe)
    for key, risky, balanced, safe in  sortMonteCarlo(ret_ind, dev_ind) :
        print "Industry {} risky : {} balanced : {} safe : {}".format(key,risky,balanced,safe)

def thing(ret_good, ret_ok, ret_bad, dev_good, dev_ok, dev_bad) :
    ret = ret_good
    dev = dev_good
    if len(ret) == 0 : ret = ret_ok
    if len(dev) == 0 : dev = dev_ok
    if len(ret) == 0 : ret = ret_bad
    if len(dev) == 0 : dev = dev_bad
    return ret, dev

def sortMonteCarlo(ret_good, dev_good) :
    risky_data = {}
    for key, stock_list in _filterMonteCarlo(**ret_good) :
        risky_data[key] = stock_list
    safe_data = {}
    for key, stock_list in _filterMonteCarlo(**dev_good) :
        safe_data[key] = stock_list
    key_list = risky_data.keys() + safe_data.keys()
    key_list = list(set(key_list))
    for key in key_list :
        risky = set(risky_data.get(key,[]))
        safe = set(safe_data.get(key,[]))
        balanced = risky.intersection(safe)
        risky = risky - balanced
        safe = safe - balanced
        yield key, list(risky), list(balanced), list(safe)

def _filterMonteCarlo(**kwargs) :
    meta = ['ret','stdev', 'sharpe']
    set_meta = set(meta)
    for key in kwargs.keys() :
        ret = pd.DataFrame()
        for value in kwargs[key] :
            p = pd.DataFrame(value).T
            ret = ret.append(p)
        mean = _reduceMonteCarlo(ret)
        mean_intersection = set_meta.intersection(set(mean.columns))
        mean = mean.drop(columns=list(mean_intersection))
        yield key, list(mean.columns)

def _reduceMonteCarlo(ret) :
    #size = int(len(ret)*.9)
    #if size < 20 : size = 20
    #ret = ret.sort_values(['stdev','sharpe']).head(size)
    ret = ret.fillna(0)
    ret = ret.mean()
    ret = pd.DataFrame(ret)
    ret.columns = ['mean']
    ret = ret[(ret['mean'] > 0.05)]
    ret = ret.T
    return ret

def _bin_by_MonteCarlo(file_list, kwargs) :
    ret_good = {}
    ret_ok = {}
    ret_bad = {}
    dev_good = {}
    dev_ok = {}
    dev_bad = {}
    for key in kwargs.keys() :
        for max_sharpe, min_dev in calculateMonteCarlo(file_list, kwargs[key]) :
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
    return ret_good, ret_ok, ret_bad, dev_good, dev_ok, dev_bad

def calculateMonteCarlo(file_list, stock_list) :
    annual = MonteCarlo.YEAR()
    sub_list = []
    data_list = pd.DataFrame() 
    for name, stock in STOCK_TIMESERIES.read(file_list, stock_list) :
        sub_list.append(name)
        data_list[name] = stock['Adj Close']
    for subset in combinations(sub_list,4) : 
        max_sharp, min_dev = annual(subset,data_list,5000) 
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
