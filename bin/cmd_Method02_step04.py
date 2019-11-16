#!/usr/bin/env python

import math
import logging
import pandas as pd

from libCommon import INI, log_exception
from libFinance import STOCK_TIMESERIES
from libSharpe import HELPER

from libDebug import trace

'''
Method 02 step 04 - Divide and Conquer

1) Partition portfolio by Risk into 3 (scaled)
2) Partition portfolio by Sharpe into 3 (scaled)

portfolios are now divided into 9 groups :
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
def prep_init() :
    key_list = ['Basic_Materials','Communication_Services','Consumer_Cyclical','Consumer_Defensive','Empty','Energy','Financial_Services','Healthcare','Industrials','Real_Estate','Technology','Utilities']
    ret = {}
    for key in key_list :
        ret[key] = {}
    default = { 'risk' : 0, 'returns': 0, 'sharpe' : 0 }
    return ret, {}, default
def _find_sector(default,values,section) :
    key = filter(lambda k : k in section, values.keys() )
    if len(key) == 0 :
       ret = default
    else :
       key = key[0]
       ret = values.get(key,default)
    if section not in ret :
          ret[section] = {}
    return ret 


def prep(*ini_list) :
    ret, overflow, default = prep_init()
    ini_list = filter(lambda x : 'step03' in x, ini_list)
    for path, section, key, value in INI.loadList(*ini_list) :
        curr = _find_sector(overflow,ret,section)
        curr[section][key] = float(value[0])
    default_keys = sorted(default)
    for key in sorted(ret) :
        portfolio_list = ret[key]
        portfolio_keys = sorted(portfolio_list)
        _list = []
        for portfolio_name in portfolio_keys :
            portfolio = portfolio_list[portfolio_name]
            value = map(lambda x : portfolio[x], default_keys)
            _list.append(dict(zip(default_keys,value)))
        yield key, dict(zip(portfolio_keys,_list))

def _partition(data,target) : 
    if data is None: return {}
    size = len(data)
    size = int(math.floor(size/3))
    if size == 0 : return {}
    low = data.sort_values([target]).head(size)
    high = data.sort_values([target]).tail(size)
    middle = data.sort_values([target]).tail(size+size).head(size)
    return dict(zip([0,1,2],[low, middle, high ]))

def partitionByRisk(data) :
    return _partition(data, 'risk')

def partitionBySharpe(data) :
    return _partition(data, 'sharpe')

def partition(data) : 
    _data = pd.DataFrame(data).T
    risk = partitionByRisk(_data) 
    low_risk = partitionBySharpe(risk.get(0,None))
    middle_risk = partitionBySharpe(risk.get(1,None))
    high_risk = partitionBySharpe(risk.get(2,None))
    ret = dict(zip([0,1,2],[low_risk,middle_risk,high_risk]))
    for _risk in sorted(ret) :
        _ret = ret[_risk]
        for _sharpe in sorted(_ret) :
            key = '{}_{}'.format(_risk,_sharpe)
            _d = _ret[_sharpe]
            yield key, _d

def transform(key, data) :
    stock_list = data.T.columns.values
    stock_list = sorted(list(stock_list))
    ret = { key : stock_list } 
    logging.info(ret)

    column_list = ['mean', 'std', 'min', 'max']

    temp = data.describe()['sharpe'] 
    logging.debug(temp)
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

def action(file_list, ini_list) : 
    for sector, portfolio_list in prep(*ini_list) :
        portfolio_list = pd.DataFrame(portfolio_list)
        logging.debug(portfolio_list)
        logging.debug(sector)
        ret = {}
        for key, data in partition(portfolio_list) :
            results = transform(key,data)
            logging.info(results)
            key_list = sorted(results)
            value_list = map(lambda x : results[x], key_list)
            key_list = map(lambda x : "{}_{}".format(sector,x), key_list)
            results = dict(zip(key_list,value_list))
            ret.update(results)
        yield sector, ret

@log_exception
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
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   ini_list = filter(lambda x : 'method02' in x, ini_list)
   file_list = env.list_filenames('local/historical_prices/*pkl')
   save_file = "{}/local/method02_step04.ini".format(env.pwd_parent)

   main(file_list,ini_list,save_file)
   print(__file__)
