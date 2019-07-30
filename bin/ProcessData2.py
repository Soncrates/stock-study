#!/usr/bin/python
import pandas as pd
from pandas import DataFrame as df
import datetime
import math
import numpy as np

from libCommon import STOCK_TIMESERIES
from libProcess import Partition

def main(control_by_quarter,*file_list) :
    stable = {}
    stable_key_list = ['label', 'mean_volume', 'mean_vol_ratio', 'std_volume', 'std_vol_ratio']
    performers = {}
    performers_key_list = ['label', 'mean_adj_close', 'mean_close_ratio', 'dev_adj_close', 'std_close_ratio']
    ini_stable = {}
    ini_performance = {}
    for test in file_list :
        test_label, test = STOCK_TIMESERIES.load(test)
        if len(test) < 100 : 
           del test
           continue
        test = Partition.by_quarter(test)
        for q in sorted(test.keys()) :
            test_vol = test[q]['Volume']
            control_vol = control_by_quarter[q]['Volume']
            comp_mean = round(test_vol.mean()/control_vol.mean(),2)
            comp_std = round(test_vol.std()/control_vol.std(),2)
            if comp_std > 1.5 : 
               continue
            if test_label not in stable.keys() : stable[test_label] = {}

            label = '{} vs {}'.format(control_label, test_label)
            value_list = [label, round(test_vol.mean(),2), comp_mean, round(test_vol.std(),2), comp_std]
            stable[test_label][q] = dict(zip(stable_key_list,value_list))
       
            test_close = test[q]['Adj Close']
            control_close = control_by_quarter[q]['Adj Close']
            comp_mean = round(test_close.mean()/control_close.mean(),2)
            comp_std = round(test_close.std()/control_close.std(),2)
            if comp_mean < 1 : 
               continue

            label = '{} vs {}'.format(control_label, test_label)
            value_list = [label, round(test_close.mean(),2), comp_mean, round(test_close.std(),2), comp_std]
            if test_label not in performers.keys() : performers[test_label] = {}
            performers[test_label][q] = dict(zip(performers_key_list,value_list))
        if test_label in stable.keys() :
           key_list = sorted(stable[test_label].keys())
           key_list = map(lambda x : str(x), key_list)
           key_list = "".join(key_list)
           key = 'Q{}'.format(key_list)
           if key not in ini_stable : 
              ini_stable[key] = []
           print "Safe ", test_label, key, stable[test_label]
           ini_stable[key].append(test_label)

        if test_label in performers.keys() :
           key_list = sorted(performers[test_label].keys())
           key_list = map(lambda x : str(x), key_list)
           key_list = "".join(key_list)
           key = 'Q{}'.format(key_list)
           if key not in ini_performance :
              ini_performance[key] = []
           print "Good ", test_label, key, performers[test_label]
           ini_performance[key].append(test_label)

        del test
    return ini_stable, ini_performance

def prep(*file_list) :
    file_list = sorted(file_list)
    spy_list = filter(lambda path : 'SPY' in path, file_list)
    control = spy_list[0]
    file_list = filter(lambda path : control != path, file_list)
    label, data = STOCK_TIMESERIES.load(control)
    print label
    quarter = Partition.by_quarter(data)
    return label, data, quarter, file_list

if __name__ == '__main__' :

   import os,sys
   from glob import glob
   from libCommon import INI

   pwd = os.getcwd()
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))
   control_label, control, control_by_quarter, file_list = prep(*file_list)
   ini_stable, ini_performance = main(control_by_quarter, *file_list)

   pwd = pw.replace('bin','local')
   config_file = '{}/better_than_spy.ini'.format(pwd)
   config = INI.init()
   INI.write_section(config,'Stability',**ini_stable)
   INI.write_section(config,'Performance',**ini_performance)
   config.write(open(config_file, 'w'))

   '''
   for q in sorted(control_by_quarter.keys()) :
       data = control_by_quarter[q]['Volume']
       print control_label, q, 'Volume mean', round(data.mean(),2), 'Volume std', round(data.std(),2)
       data = control_by_quarter[q]['Adj Close']
       print control_label, q, 'Adj Close mean', round(data.mean(),2), 'Adj Close std', round(data.std(),2)
   '''
