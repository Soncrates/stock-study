#!/usr/bin/python


import os,sys
import logging
import pandas as pd

pwd = os.getcwd()
pwd = pwd.replace('test','bin')
sys.path.append(pwd)

from libMonteCarlo import MonteCarlo
from libCommon import INI_READ as READ
from libUtils import combinations
from libFinance import STOCK_TIMESERIES

def init(*ini_list) :
    performers = {}
    stability = {}
    for file_name, name, key, value in READ.read(*ini_list) :
        config = None
        if name == "Stability" :
           config = stability
        if name == "Performance" :
           config = performers
        config[key] = value
    ret = performers.get('Q1234',[])
    return ret

def read(file_list, stock_list) :
    for path in file_list :
        name, ret = STOCK_TIMESERIES.load(path)
        if name not in stock_list : 
           del ret      
           continue
        yield name, ret

def prep(*file_list) :
   file_list = sorted(file_list)
   spy_list = filter(lambda x : 'SPY' in x, file_list)
   spy = spy_list[0]
   name, data = STOCK_TIMESERIES.load(spy)
   return name, data

def main(file_list, stock_list) :
    stock_list = init(*ini_list)
    name, spy_data = prep(*file_list) 

    annual = MonteCarlo.YEAR()

    stock_data = pd.DataFrame() 
    stock_data[name] = spy_data['Adj Close']

    max_sharp, min_vol = annual([name],stock_data) 
    logging.info( max_sharp)
    logging.info( min_vol)

    stock_name = []
    for name, stock in read(file_list, stock_list) :
        stock_name.append(name)
        stock_data[name] = stock['Adj Close']
    for subset in combinations(stock_name) : 
        logging.info(subset)
        max_sharp, min_vol = annual(subset,stock_data) 
        logging.info(max_sharp)
        logging.info(min_vol)

if __name__ == '__main__' :

   from glob import glob

   pwd = os.getcwd()
   pwd = pwd.replace('test','local')
   ini_list = glob('{}/*.ini'.format(pwd))
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))
   main(file_list, ini_list)
