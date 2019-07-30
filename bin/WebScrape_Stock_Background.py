#!/usr/bin/python

from libWeb import YAHOO_PROFILE
from libCommon import INI, NASDAQ

def main(finder,profile) :
    ini_sector = {}
    ini_industry = {}
    ini_Category = {}
    ini_FundFamily = {}
    for stock in finder() :
        print stock
        ret = profile(stock)
        curr = None
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
            value = ret[key]
            value = value.replace(' ', '_')
            value = value.replace('--', '-')
            value = value.replace('_-_', '_')
            value = value.replace('(', '')
            value = value.replace(')', '')
            value = value.replace('O&#39;', '')
            value = value.replace('_&_', '_and_')
            value = value.replace('&', 'and')
            value = value.replace('%', '_percent')
            value = value.replace(',_LLC', '_LLC')
            if value not in curr :
               curr[value] = []
            curr[value].append(stock)
    return ini_sector, ini_industry, ini_Category, ini_FundFamily

if __name__ == '__main__' :

   import os,sys

   pwd = os.getcwd()
   pwd = pwd.replace('bin','local')
   stock_ini = '{}/stock_meta.ini'.format(pwd)
   nasdaq = '{}/{}'.format(pwd,NASDAQ.path)
   nasdaq = NASDAQ.init(filename=nasdaq)
   profile = YAHOO_PROFILE()

   ini_sector, ini_industry, ini_Category, ini_FundFamily = main(nasdaq,profile)

   config = INI.init()
   INI.write_section(config,'Sector',**ini_sector)
   INI.write_section(config,'Industry',**ini_industry)
   INI.write_section(config,'Category',**ini_Category)
   INI.write_section(config,'FundFamily',**ini_FundFamily)
   config.write(open(stock_ini, 'w'))
   '''
   for name, key, value in INI.read_section(config) : 
       print name, key, value
   '''
