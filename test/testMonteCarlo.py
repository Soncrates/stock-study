#!/usr/bin/python

#import sys
#sys.path.append(sys.path[0].replace('test','bin'))
from itertools import combinations as iter_combo

import context
from libCommon import INI
from libUtils import log_exception
from libFinance import STOCK_TIMESERIES
from libSharpe import HELPER as MONTECARLO

def combinations(stock_list,size=5) :
    logging.info( stock_list)
    ret_list = iter_combo(stock_list,size)
    for ret in list(ret_list):
        yield list(ret)

def init(*ini_list) :
    performers = {}
    stability = {}
    for file_name, name, key, value in INI.loadList(*ini_list) :
        config = None
        if name == "Stability" :
           config = stability
        elif name == "Performance" :
           config = performers
        else : continue
        config[key] = value
    ret = performers.get('Q1234',[])
    return ret

def prep() :
    target = 'file_list'
    file_list = globals().get(target,[])
    file_list = sorted(file_list)
    spy_list = filter(lambda path : 'SPY' in path, file_list)
    if not isinstance(spy_list,list) :
       spy_list = list(spy_list)
    control = spy_list[0]
    file_list = filter(lambda path : control != path, file_list)
    if not isinstance(file_list,list) :
       file_list = list(file_list)
    label, data = STOCK_TIMESERIES.load(control)
    return label, data, file_list

def action(stock_list) :
    target = 'file_list'
    file_list = globals().get(target,[])
    for path in file_list :
        name, ret = STOCK_TIMESERIES.load(path)
        if name not in stock_list : 
           del ret      
           continue
        yield name, ret

@log_exception
def main(stock_list) :
   name, spy_data, file_list = prep()

   annual = MONTECARLO.YEAR()
   _ret, _dev, _sharpe, _length= annual.single(spy_data['Adj Close']) 
   logging.info( "{} return {}, dev {}, sharpe {} length {}".format(name,_ret,_dev,_sharpe, _length))

   perf_list = init(*ini_list)
   for name, data in action(perf_list) :
       ret, dev, sharpe, length = annual.single(data['Adj Close']) 
       flag_long = length > 250
       flag_stable = dev < 3*_dev
       flag_profit = ret > _ret
       flag_sharpe = sharpe > _sharpe
       if flag_long and flag_stable and flag_profit and flag_sharpe :
          logging.info( "{} return {}, dev {}, sharpe {}, length {}".format(name,ret,dev,sharpe,length))
       del data

if __name__ == '__main__' :

   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()

   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   main(ini_list)
