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

def prep(ini_list, portfolio_list) :
    logging.info(ini_list)
    logging.info(portfolio_list)

    target_sector = 'target_sector'
    target_sector = globals().get(target_sector,'_')
    target_files = 'file_list'
    target_files = globals().get(target_files,[])

    for path, sector_enum, key, value in INI.loadList(*ini_list) :
        if not key.endswith(target_sector)  : 
            continue
        logging.debug((key,value))
        for _path, portfolio_name, stock_name, weight in INI.loadList(*portfolio_list) :
            if portfolio_name not in value :
               continue
            logging.debug((stock_name,weight))
            weight = float(weight[0])
            #weight = round(weight,3)
            yield sector_enum, portfolio_name, stock_name, weight

def action(input_file, ini_list) : 
    portfolio_list = filter(lambda name : 'step03' in name, ini_list)
    logging.info(portfolio_list)
    ret = {}
    for sector_enum, portfolio_name, stock_name, weight in prep(input_file,portfolio_list) :
        logging.debug((sector_enum, portfolio_name, stock_name, weight))
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
@trace
def main(input_file, ini_list,local_dir) : 
    for name, section_list in action(input_file, ini_list) :
        output_file = "{}/portfolio_{}.ini".format(local_dir, name)
        output_file = output_file.replace(" ", "_")
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
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   file_list = env.list_filenames('local/historical_prices/*pkl')
   ini_list = env.list_filenames('local/method02*.ini')
   input_file = filter(lambda x : 'step04' in x, ini_list)
   local_dir = "{}/local".format(env.pwd_parent)
   target_sector = '_0_2'
   #target_sector = '_1_2'

   main(input_file, ini_list,local_dir)
