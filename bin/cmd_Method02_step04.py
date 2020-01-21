#!/usr/bin/env python

import math
import logging
import pandas as pd

from libCommon import INI, exit_on_exception
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
    target = 'sector_list'
    sector_list = globals().get(target,[])
    logging.info(sector_list)
    ret = {}
    for key in sector_list :
        ret[key] = {}
    default = { 'risk' : 0, 'returns': 0, 'sharpe' : 0 }
    return ret, {}, default

def lambdaFindSector(default,values,section) :
    key = filter(lambda k : k in section, values.keys() )
    if len(key) == 0 :
       ret = default
       logging.warn("Unrecognized section : {}".format(section))
    else :
       key = key[0]
       ret = values.get(key,default)
    if section not in ret :
          ret[section] = {}
    return ret 

def prep(*ini_list) :
    ret, overflow, default = prep_init()
    for path, section, key, value in INI.loadList(*ini_list) :
        curr = lambdaFindSector(overflow,ret,section)
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
        ret = cls._partition(data, 'risk')
        logging.info(ret)
        return ret

    @classmethod
    def partitionBySharpe(cls, data) :
        ret =  cls._partition(data, 'sharpe')
        logging.info(ret)
        return ret

    @classmethod
    def partition(cls, data) : 
        _data = pd.DataFrame(data).T
        risk = cls.partitionByRisk(_data) 
        low_risk = cls.partitionBySharpe(risk.get(0,None))
        middle_risk = cls.partitionBySharpe(risk.get(1,None))
        high_risk = cls.partitionBySharpe(risk.get(2,None))
        if len(low_risk) == 0 :
           logging.warn("Partition yielded no results")
           logging.warn(risk)
           low_risk = risk
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
        logging.info(sector)
        ret = {}
        for key, data in HELPER.partition(portfolio_list) :
            results = HELPER.transform(key,data)
            logging.info(results)
            key_list = sorted(results)
            value_list = map(lambda x : results[x], key_list)
            key_list = map(lambda x : "{}_{}".format(sector,x), key_list)
            results = dict(zip(key_list,value_list))
            ret.update(results)
        yield sector, ret

@exit_on_exception
@trace
def main(file_list, ini_list,save_file) : 
    logging.info("loading results {}".format(ini_list))
    ret = INI.init()
    for key, value in action(file_list, ini_list) :
        logging.debug(value)
        INI.write_section(ret,key,**value)
    ret.write(open(save_file, 'w'))
    logging.info("results saved to {}".format(save_file))

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/method02*.ini')
   ini_list = filter(lambda x : 'step03' in x, ini_list)
   file_list = env.list_filenames('local/historical_prices/*pkl')
   save_file = "{}/local/method02_step04.ini".format(env.pwd_parent)

   sector_list = ['Basic Materials','Communication Services','Consumer Cyclical','Consumer Defensive','Energy','Financial Services','Healthcare','Industrials','Real Estate','Technology','Utilities']
   main(file_list,ini_list,save_file)
