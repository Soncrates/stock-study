#!/usr/bin/env python

import math
import logging
import pandas as pd

from libCommon import INI, log_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import HELPER as MONTECARLO

from libDebug import trace, cpu

'''
Method 02 step 01 - Divide and Conquer

1) Partition stocks by Sector (enumerated)
2) Partition stocks by Risk into 3 (scaled)
3) Partition stocks by Sharpe into 3 (scaled)

Each sector is now divided into 9 groups :
0_0 : low risk, low sharpe
0_1 : low risk, medium sharpe
0_2 : low risk, high sharpe
1_0 : medium risk, low sharpe
1_1 : medium risk, medium sharpe
1_2 : medium risk, high sharpe
2_0 : high risk, low sharpe
2_1 : high risk, medium sharpe
2_2 : high risk, high sharpe

Write results and basic statistics data about each sub section into ini file
'''
class HELPER() :
    @classmethod
    def _partition(cls, data,target) : 
        if data is None: return {}
        size = len(data)
        size = int(math.floor(size/3))
        if size == 0 : return {}
        low = data.sort_values([target]).head(size)
        high = data.sort_values([target]).tail(size)
        middle = data.sort_values([target]).tail(size+size).head(size)
        return dict(zip([0,1,2],[low, middle, high ]))

    @classmethod
    def partitionByRisk(cls, data) :
        return cls._partition(data, 'risk')

    @classmethod
    def partitionBySharpe(cls, data) :
        return cls._partition(data, 'sharpe')

    @classmethod
    def partition(cls, data) : 
        _data = pd.DataFrame(data).T
        risk = cls.partitionByRisk(_data) 
        low_risk = cls.partitionBySharpe(risk.get(0,None))
        middle_risk = cls.partitionBySharpe(risk.get(1,None))
        high_risk = cls.partitionBySharpe(risk.get(2,None))
        ret = dict(zip([0,1,2],[low_risk,middle_risk,high_risk]))
        for _risk in sorted(ret) :
            _ret = ret[_risk]
            for _sharpe in sorted(_ret) :
                key = '{}_{}'.format(_risk,_sharpe)
                _d = _ret[_sharpe]
                yield key, _d

    @classmethod
    def transform(cls, key, data) :
        stock_list = data.T.columns.values
        stock_list = sorted(list(stock_list))
        ret = { key : stock_list } 

        column_list = ['mean', 'std', 'min', 'max']

        temp = data.describe()['sharpe'] 
        value_list = map(lambda key : round(temp[key],2), column_list)
        key_list = map(lambda k : '{}_sharpe_{}'.format(key,k), column_list)
        temp = dict(zip(key_list,value_list))
        ret.update(temp)

        temp = data.describe()['risk'] 
        value_list = map(lambda key : round(temp[key],2), column_list)
        key_list = map(lambda k : '{}_risk_{}'.format(key,k), column_list)
        temp = dict(zip(key_list,value_list))
        ret.update(temp)
        return ret

def prep(*ini_list) :
    for path, section, key, stock in INI.loadList(*ini_list) :
        yield key, stock

def load(file_list, value_list) :
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        data = MONTECARLO.find(data['Adj Close'], period=FINANCE.YEAR)
        #filter stocks that have less than a year
        sharpe = data.get('sharpe',0)
        if sharpe == 0 : continue
        #filter stocks that have negative returns
        returns = data.get('returns',0)
        if returns <= 0 : continue
        key_list = data.keys()
        value_list = map(lambda x : data[x], key_list)
        value_list = map(lambda x : round(x,2), value_list)
        msg = dict(zip(key_list,value_list))
        logging.info((name, msg))
        ret[name] = data
    return ret

@cpu
def action(file_list, ini_list) : 
    for sector, _stock_list in prep(*ini_list) :
        logging.debug(sector)
        logging.debug(_stock_list)
        stock_list = load(file_list,_stock_list)
        ret = {}
        for key, data in HELPER.partition(stock_list) :
            results = HELPER.transform(key,data)
            key_list = sorted(results)
            value_list = map(lambda x : results[x], key_list)
            key_list = map(lambda x : "{}_{}".format(sector,x), key_list)
            results = dict(zip(key_list,value_list))
            ret.update(results)
        yield sector, ret

@log_exception
@trace
def main(file_list, ini_list,save_file) : 
    ret = INI.init()
    for key, value in action(file_list, ini_list) :
        logging.debug(value)
        INI.write_section(ret,key,**value)
    ret.write(open(save_file, 'w'))

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   ini_list = filter(lambda x : 'background' in x, ini_list)
   ini_list = filter(lambda x : 'stock_background' in x, ini_list)
   file_list = env.list_filenames('local/historical_prices/*pkl')
   save_file = "{}/local/method02_step01.ini".format(env.pwd_parent)

   main(file_list,ini_list,save_file)
