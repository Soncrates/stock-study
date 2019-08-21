#!/usr/bin/python

import logging
from multiprocessing import Pool
from libWeb import YAHOO_PROFILE, worker
from libCommon import INI, NASDAQ

'''
   Web Scraper
   Use RESTful interface to to get web pages and parse for relevant info about stocks and funds
'''
def formatting(ret) :
    if not isinstance(ret,basestring) : 
       ret = str(ret)
    ret = ret.replace(' ', '_')
    ret = ret.replace('--', '-')
    ret = ret.replace('_-_', '_')
    ret = ret.replace('(', '')
    ret = ret.replace(')', '')
    ret = ret.replace('O&#39;', '')
    ret = ret.replace('_&_', '_and_')
    ret = ret.replace('&', 'and')
    ret = ret.replace('%', '_percent')
    ret = ret.replace(',_LLC', '_LLC')
    if len(ret) == 0 : ret = "Empty"
    return ret

def _append(stock, main, sub, **group) :
    if main not in group: 
       group[main] = {}
    if sub not in group[main] : 
       group[main][sub] = []
    group[main][sub].append(stock)
    return group

def main(finder, profile) :
    try :
        return _main(finder, profile)
    except Exception as e :
        logging.error(e, exc_info=True)

def sync(stock):
    return YAHOO_PROFILE.get(stock)

def chunking(finder) :
    stock_list = []
    pool = Pool(15)
    for stock in finder() :
        stock_list.append(stock)
        if len(stock_list) < 60 : continue
        logging.info(stock_list)
        result_list = pool.map(sync,stock_list)
        for result in result_list :
            yield result
        del result_list
        del stock_list
        stock_list = []

def _main(finder, profile) :
    ini_sector = {}
    ini_industry = {}
    ini_Category = {}
    ini_FundFamily = {}
    group_by_sector = {}
    group_by_industry = {}
    empty_list = []
    for ret in chunking(finder) :
        curr = None
        stock = ret.get('Stock', None)
        if not stock : continue
        Sector = ret.get('Sector','Fund')
        Sector = formatting(Sector)
        Industry = ret.get('Industry','Fund')
        Industry = formatting(Industry)
        if Sector != Industry :
           group_by_sector = _append(stock, Sector, Industry, **group_by_sector)
           group_by_industry = _append(stock, Industry, Sector, **group_by_industry)
        elif Sector == 'Empty' : 
             empty_list.append(stock)
        elif Sector != 'Fund' : 
             print Sector, Industry
        for key in ret.keys() :
            if key == 'Sector' :
               curr = ini_sector
            elif key == 'Industry' :
               curr = ini_industry
            elif key == 'Category' :
               curr = ini_Category
            elif key == 'Fund Family' :
               curr = ini_FundFamily
            else :
               continue
            value = formatting(ret[key])
            if value not in curr :
               curr[value] = []
            curr[value].append(stock)
    key_list = ['Sector', 'Industry', 'Category', 'Fund']
    value_list = [ini_sector, ini_industry, ini_Category, ini_FundFamily]
    ret = dict(zip(key_list,value_list))
    key_list = ['Background', 'SectorGroupBy', 'IndustryGroupBy']
    value_list = [ret, group_by_sector, group_by_industry]
    ret = dict(zip(key_list,value_list))
    return ret, empty_list

if __name__ == '__main__' :

   import os,sys
   from libCommon import TIMER

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   pwd = pwd.replace('bin','local')
   nasdaq = '{}/{}'.format(pwd,NASDAQ.path)
   nasdaq = NASDAQ.init(filename=nasdaq)
   profile = YAHOO_PROFILE()

   logging.info("started {}".format(name))
   elapsed = TIMER.init()
   ini, empty_list = main(nasdaq,profile)
   logging.info("finished {} elapsed time : {}".format(name,elapsed()))

   print empty_list

   stock_ini = '{}/nasdaq_background.ini'.format(pwd)
   config = INI.init()
   temp = ini['Background']
   for key in sorted(temp.keys()) :
       INI.write_section(config,key,**temp[key])
   config.write(open(stock_ini, 'w'))

   stock_ini = '{}/nasdaq_background_sector.ini'.format(pwd)
   config = INI.init()
   temp = ini['SectorGroupBy']
   for key in sorted(temp.keys()) :
       INI.write_section(config,key,**temp[key])
   config.write(open(stock_ini, 'w'))

   '''
   # At one time, industries contained different sectors, this is no longer the case

   stock_ini = '{}/nasdaq_background_industry.ini'.format(pwd)
   config = INI.init()
   temp = ini['IndustryGroupBy']
   for key in sorted(temp.keys()) :
       INI.write_section(config,key,**temp[key])
   config.write(open(stock_ini, 'w'))
   '''
