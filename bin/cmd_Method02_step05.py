#!/usr/bin/env python

import math
import logging
import pandas as pd

from libCommon import INI, log_exception
from libFinance import STOCK_TIMESERIES
from libSharpe import HELPER

from libDebug import trace

'''
Method 02 step 05 - 

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
def lambdaFindSector(default,values,section) :
    key = filter(lambda k : k in section, values.keys() )
    if len(key) == 0 :
       ret = default
    else :
       key = key[0]
       ret = values.get(key,default)
    if section not in ret :
          ret[section] = {}
    return ret 

def prep(ini_list, portfolio_list) :
    #ret, overflow, default = prep_init()
    logging.info(ini_list)
    logging.info(portfolio_list)
    for path, sector_enum, key, value in INI.loadList(*ini_list) :
        if not key.endswith("_0_2")  : 
            continue
        for _path, portfolio_name, stock_name, weight in INI.loadList(*portfolio_list) :
            if portfolio_name not in value :
               continue
            weight = float(weight[0])
            #weight = round(weight,3)
            yield sector_enum, portfolio_name, stock_name, weight

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

def action(input_file, file_list, ini_list) : 
    portfolio_list = filter(lambda name : '03' in name, ini_list)
    logging.info(portfolio_list)
    ret = {}
    for sector_enum, portfolio_name, stock_name, weight in prep(input_file,portfolio_list) :
        curr = None
        if sector_enum not in ret :
           ret[sector_enum] = {}
        curr = ret[sector_enum]
        if portfolio_name not in curr :
           curr[portfolio_name] = {}
        curr = curr[portfolio_name]
        curr[stock_name] = weight
    sector_enum = sorted(ret.keys())
    portfolio_list = map(lambda sector : ret[sector], sector_enum)
    for i, sector_name in enumerate(sector_enum) :
        yield sector_name, portfolio_list[i]
        for j, value in enumerate(portfolio_list[i]) :
            logging.info((i,j, sector_name,value))

@log_exception
def main(env, input_file, file_list, ini_list,output_file) : 
    for name, section_list in action(input_file, file_list, ini_list) :
        output_file = "{}/local/portfolio_{}.ini".format(env.pwd_parent,name)
        ret = INI.init()
        name_list = sorted(section_list.keys())
        value_list = map(lambda key : section_list[key], name_list)
        for i, name in enumerate(name_list) :
            INI.write_section(ret,name,**value_list[i])
        ret.write(open(output_file, 'w'))

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
   input_file = env.list_filenames('local/*step04.ini')
   output_file = "{}/local/method02_step05.ini".format(env.pwd_parent)

   main(env, input_file, file_list,ini_list,output_file)
