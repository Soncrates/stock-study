#!/usr/bin/python
import pandas as pd
from pandas import DataFrame as df
import datetime
import math
import numpy as np

import os,sys
pwd = os.getcwd()
pwd = pwd.replace('test','bin')
sys.path.append(pwd)

from libFinance import STOCK_TIMESERIES
from libProcess import CompareStock, StockTransform, Monthly_Transform

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

def main(*file_list) :
    q = []
    name = None
    pair_list = []
    for key, contender_name, contender_data, contender_quarter, spy_name, spy_data, spy_quarter in loadData(*file_list) :
        if key == 1 :
           if isinstance(name,basestring) :
              logging(( name, q, pair_list))
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
           logging.info( contender_quarter)
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

if __name__ == '__main__' :

   from glob import glob

   import os,sys
   pwd = os.getcwd()
   pwd = pwd.replace('test','local')
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))
   main(*file_list)
   logging.info( "Done")
