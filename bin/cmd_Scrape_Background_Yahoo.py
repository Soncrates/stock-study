#!/usr/bin/env python

import logging
from libWeb import YAHOO_PROFILE as PROFILE
from libCommon import INI, log_exception
from libDebug import trace

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

@log_exception
def debug_from_cmd_line(*stock_list) :
    stock_list = map(lambda stock : PROFILE.get(stock), stock_list)
    ini, empty_list = find(stock_list)
    for key in sorted(ini) :
        print( "{}.ini".format(key))
        _1 = ini[key]
        for key1 in sorted(_1) :
            print("[{}]".format(key1))
            _2 = _1[key1]
            for key2 in sorted(_2) :
                value = ",".join(_2[key2])
                print("{} = {}".format(key2,value))
    return empty_list

@log_exception
@trace
def main() :

    env = globals().get('env',None)
    nasdaq = '{}/local/{}'.format(env.pwd_parent, NASDAQ.path)
    finder = NASDAQ.init(filename=nasdaq)

    stock_list = map(lambda stock : PROFILE.get(stock), finder())
    ini, empty_list = find(stock_list)
    stock_ini = '{pwd_parent}/local/yahoo_background.ini'.format(**vars(env))
    config = INI.init()
    temp = ini['Background']
    for key in sorted(temp.keys()) :
        INI.write_section(config,key,**temp[key])
    config.write(open(stock_ini, 'w'))

    stock_ini = '{pwd_parent}/local/yahoo_background_sector.ini'.format(**vars(env))
    config = INI.init()
    temp = ini['SectorGroupBy']
    for key in sorted(temp.keys()) :
        INI.write_section(config,key,**temp[key])
    config.write(open(stock_ini, 'w'))

    # At one time, industries contained different sectors, this is no longer the case

    stock_ini = '{pwd_parent}/local/yahoo_background_industry.ini'.format(**vars(env))
    config = INI.init()
    temp = ini['IndustryGroupBy']
    for key in sorted(temp.keys()) :
        INI.write_section(config,key,**temp[key])
    config.write(open(stock_ini, 'w'))
    return empty_list

@trace
def find(stock_list) :
    ini_sector = {}
    ini_industry = {}
    ini_Category = {}
    ini_FundFamily = {}
    group_by_sector = {}
    group_by_industry = {}
    empty_list = []
    for ret in stock_list :
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

   import logging
   from libCommon import ENVIRONMENT, NASDAQ

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   stock_list = env.argv[1:]
   if len(stock_list) > 0 :
      debug_from_cmd_line(*stock_list)
   else :
      empty_list = main()
      if len(empty_list) > 0 :
         print("No DATA F0R {}".format(empty_list))
