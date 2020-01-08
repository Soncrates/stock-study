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
class HELPER_THIRDS() :
    @classmethod
    def base_partition(cls, data,target) :
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
        ret = cls.base_partition(data, 'risk')
        for msg in HELPER._debug(ret) :
            logging.info((len(msg),msg))
        return ret

    @classmethod
    def partitionBySharpe(cls, data) :
        ret = cls.base_partition(data, 'sharpe')
        for msg in HELPER._debug(ret) :
            logging.info((len(msg),msg))
        return ret
    @classmethod
    def _partition(cls, data) :
        risk = cls.partitionByRisk(data)
        low_risk = cls.partitionBySharpe(risk.get(0,None))
        middle_risk = cls.partitionBySharpe(risk.get(1,None))
        high_risk = cls.partitionBySharpe(risk.get(2,None))
        if len(low_risk) == 0 :
           logging.warn("Partition yielded no results")
           logging.warn(risk)
           low_risk = risk

        ret = dict(zip([0,1,2],[low_risk,middle_risk,high_risk]))
        label = ['A1','A2','A3','B1','B2','B3','C1','C2','C3']
        idx = 0
        for _risk in sorted(ret) :
            _ret = ret[_risk]
            for _sharpe in sorted(_ret) :
                key = label[idx]
                _d = _ret[_sharpe]
                idx += 1
                yield key, _d
    @classmethod
    def partition(cls, data) :
        for msg in HELPER._debug(data) :
            logging.info((len(msg),msg))
        data = pd.DataFrame(data).T
        for key, data in cls._partition(data) :
            yield key, data

class HELPER_HALVES() :
    @classmethod
    def base_partition(cls, data,target) :
        if data is None: return {}
        size = len(data)
        size = int(math.floor(size/2))
        if size == 0 : return {}
        low = data.sort_values([target]).head(size)
        high = data.sort_values([target]).tail(size)
        return dict(zip([0,1],[low, high ]))

    @classmethod
    def partitionByRisk(cls, data) :
        ret = cls.base_partition(data, 'risk')
        for msg in HELPER._debug(ret) :
            logging.info((len(msg),msg))
        return ret

    @classmethod
    def partitionBySharpe(cls, data) :
        ret = cls.base_partition(data, 'sharpe')
        for msg in HELPER._debug(ret) :
            logging.info((len(msg),msg))
        return ret

    @classmethod
    def _partition(cls, data) :
        risk = cls.partitionByRisk(data)
        low_risk = cls.partitionBySharpe(risk.get(0,None))
        high_risk = cls.partitionBySharpe(risk.get(1,None))
        if len(low_risk) == 0 :
           logging.warn("Partition yielded no results")
           logging.warn(risk)
           low_risk = risk

        ret = dict(zip([0,1],[low_risk,high_risk]))
        label = ['Z1','Z2','Y1','Y2']
        idx = 0
        for _risk in sorted(ret) :
            _ret = ret[_risk]
            for _sharpe in sorted(_ret) :
                key = label[idx]
                _d = _ret[_sharpe]
                idx += 1
                yield key, _d

    @classmethod
    def partition(cls, data) :
        for msg in HELPER._debug(data) :
            logging.info((len(msg),msg))
        data = pd.DataFrame(data).T
        for key, data in cls._partition(data) :
            yield key, data

class HELPER() :

    @classmethod
    def transform(cls, key, data) :
        stock_list = data.T.columns.values
        stock_list = sorted(list(stock_list))
        ret = { key + "_stocks" : stock_list } 

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

    @classmethod
    def _debug(cls, data) :
        for v in data.values() :
            if isinstance(v,dict) :
              yield sorted(data)
              break
            else :
              yield sorted(v.T.columns.values)

    @classmethod
    def round_values(cls, **data) : 
        key_list = data.keys()
        value_list = map(lambda x : data[x], key_list)
        value_list = map(lambda x : round(x,2), value_list)
        ret = dict(zip(key_list,value_list))
        return ret

    @classmethod
    def rename_keys(cls, sector, **data) : 
        key_list = sorted(data)
        value_list = map(lambda x : data[x], key_list)
        key_list = map(lambda x : "{}_{}".format(sector,x), key_list)
        ret = dict(zip(key_list,value_list))
        return ret

    @classmethod
    def is_stock_invalid(cls, name, data) :
        if len(data) < 7*FINANCE.YEAR :
           logging.info("{} of length {} rejected for being less than {}".format(name,len(data),7*FINANCE.YEAR))
           return True, None
        ret = MONTECARLO.find(data['Adj Close'], period=FINANCE.YEAR)
        sharpe = ret.get('sharpe',0)
        if sharpe <= 0.8 :
           logging.info("{} of sharpe ratio {} rejected for being less stable than SPY".format(name,sharpe))
           return True, None
        returns = ret.get('returns',0)
        if returns <= 0.02 : 
           logging.info("{} of returns {} rejected for being less profitable than bonds".format(name,returns))
           return True, None
        return False, ret

def prep() :
    target = 'ini_list'
    ini_list = globals().get(target,[])
    logging.info("loading results {}".format(ini_list))
    for path, section, key, stock_list in INI.loadList(*ini_list) :
        yield key, stock_list

def load(value_list) :
    target = "file_list"
    file_list = globals().get(target,[])
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        flag, data = HELPER.is_stock_invalid(name, data) 
        if flag :
            continue
        ret[name] = data
        msg = HELPER.round_values(**data)
        logging.info((name, msg))
    return ret

def _partition(*stock_list) :
    flag = len(stock_list)
    if flag > 27 :
       return HELPER_THIRDS.partition
    elif flag > 12 :
       return HELPER_HALVES.partition
    return None

def _action(stock_list) : 
    partition = _partition(*stock_list)
    if partition is None :
       yield "0", stock_list
       return
    for key, data in partition(stock_list) :
        _stock_list = data.T.columns.values
        _stock_list = load(_stock_list)
        for _key, _data in _action(_stock_list) :
            _key = "{}_{}".format(key,_key)
            yield _key, _data
            
def action() : 
    for sector, _stock_list in prep() :
        logging.debug(sector)
        logging.debug(_stock_list)
        stock_list = load(_stock_list)

        ret = {}
        for key, data in _action(stock_list) :
            logging.info(len(data))
            data = load(data)
            data = pd.DataFrame(data).T
            results = HELPER.transform(key,data)
            results = HELPER.rename_keys(sector, **results)
            ret.update(results)
        yield sector, ret

@log_exception
@trace
def main(save_file) : 
    ret = INI.init()
    for key, value in action() :
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

   ini_list = env.list_filenames('local/*.ini')
   ini_list = filter(lambda x : 'background' in x, ini_list)
   ini_list = filter(lambda x : 'stock_background' in x, ini_list)
   file_list = env.list_filenames('local/historical_prices/*pkl')
   save_file = "{}/local/method03_step01.ini".format(env.pwd_parent)

   main(save_file)
