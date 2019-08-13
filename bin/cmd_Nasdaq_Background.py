#!/usr/bin/python

from collections import Counter
from libWeb import YAHOO_PROFILE
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

def main(finder, profile) :
    ini_sector = {}
    ini_industry = {}
    ini_Category = {}
    ini_FundFamily = {}
    group_by_sector = {}
    group_by_industry = {}
    empty_list = []
    for stock in finder() :
        print stock
        ret = profile(stock)
        curr = None
        Sector = ret.get('Sector','Fund')
        Sector = formatting(Sector)
        Industry = ret.get('Industry','Fund')
        Industry = formatting(Industry)
        if Sector != Industry :
           if Sector not in group_by_sector : 
              group_by_sector[Sector] = {}
           if Industry not in group_by_sector[Sector] : 
              group_by_sector[Sector][Industry] = []
           if Industry not in group_by_industry : 
              group_by_industry[Industry] = {}
           if Sector not in group_by_industry[Industry] : 
              group_by_industry[Industry][Sector] = []
           group_by_industry[Industry][Sector].append(stock)
           group_by_sector[Sector][Industry].append(stock)
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

   pwd = os.getcwd()
   pwd = pwd.replace('bin','local')
   nasdaq = '{}/{}'.format(pwd,NASDAQ.path)
   nasdaq = NASDAQ.init(filename=nasdaq)
   profile = YAHOO_PROFILE()

   ini, empty_list = main(nasdaq,profile)
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

   stock_ini = '{}/nasdaq_background_industry.ini'.format(pwd)
   config = INI.init()
   temp = ini['IndustryGroupBy']
   for key in sorted(temp.keys()) :
       INI.write_section(config,key,**temp[key])
   config.write(open(stock_ini, 'w'))
   '''
   for name, key, value in INI.read_section(config) : 
       print name, key, value
   '''
