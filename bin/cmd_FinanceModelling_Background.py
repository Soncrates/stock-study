#!/usr/bin/env python

import logging
from multiprocessing import Pool
from libWeb import FINANCEMODELLING_STOCK_LIST as STOCK_LIST, FINANCEMODELLING_PROFILE as PROFILE
from libCommon import INI

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

def main() :
    try :
        return _main()
    except Exception as e :
        logging.error(e, exc_info=True)

def sync(stock):
    return PROFILE.get(stock)

def chunking() :
    stock_list = []
    pool = Pool(15)
    for stock in STOCK_LIST.get() :
        target = 'symbol'
        stock = stock.get(target,None)
        if not stock : continue
        stock_list.append(stock)
        if len(stock_list) < 60 : continue
        logging.info(stock_list)
        result_list = pool.map(sync,stock_list)
        for result in result_list :
            yield result
        del result_list
        del stock_list
        stock_list = []

def _main() :
    ini_sector = {}
    ini_industry = {}
    group_by_sector = {}
    group_by_industry = {}
    empty_list = []
    for ret in chunking() :
        curr = None
        stock = ret.get('Stock', None)
        if not stock : continue
        Sector = ret.get('sector','Fund')
        Sector = formatting(Sector)
        Industry = ret.get('industry','Fund')
        Industry = formatting(Industry)
        if Sector != Industry :
           group_by_sector = _append(stock, Sector, Industry, **group_by_sector)
           group_by_industry = _append(stock, Industry, Sector, **group_by_industry)
        elif Sector == 'Empty' : 
             empty_list.append(stock)
        elif Sector != 'Fund' : 
             print Sector, Industry
        for key in ret.keys() :
            if key == 'sector' :
               curr = ini_sector
            elif key == 'industry' :
               curr = ini_industry
            else :
               continue
            value = formatting(ret[key])
            if value not in curr :
               curr[value] = []
            curr[value].append(stock)
    key_list = ['Sector', 'Industry']
    value_list = [ini_sector, ini_industry]
    ret = dict(zip(key_list,value_list))
    key_list = ['Background', 'SectorGroupBy', 'IndustryGroupBy']
    value_list = [ret, group_by_sector, group_by_industry]
    ret = dict(zip(key_list,value_list))
    return ret, empty_list

if __name__ == '__main__' :

   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   ini, empty_list = main()

   print empty_list

   stock_ini = '{}/local/prototype_background.ini'.format(env.pwd_parent)
   config = INI.init()
   temp = ini['Background']
   for key in sorted(temp.keys()) :
       INI.write_section(config,key,**temp[key])
   config.write(open(stock_ini, 'w'))

   stock_ini = '{}/local/prototype_background_sector.ini'.format(env.pwd_parent)
   config = INI.init()
   temp = ini['SectorGroupBy']
   for key in sorted(temp.keys()) :
       INI.write_section(config,key,**temp[key])
   config.write(open(stock_ini, 'w'))

   # At one time, industries contained different sectors, this is no longer the case

   stock_ini = '{}/local/nasdaq_background_industry.ini'.format(env.pwd_parent)
   config = INI.init()
   temp = ini['IndustryGroupBy']
   for key in sorted(temp.keys()) :
       INI.write_section(config,key,**temp[key])
   config.write(open(stock_ini, 'w'))
