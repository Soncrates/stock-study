import logging
from libCommon import INI_READ as INI

'''
   Helper Functions
   Using ini files to save states between programs
   DEPRECATED ???
'''
def getByNasdaq(*ini_list) :
    Sector = {}
    Industry = {}
    FundFamily = {}
    Category = {}

    ini_list = filter(lambda file : "yahoo" in file, ini_list)
    ini_list = filter(lambda file : "background" in file, ini_list)
    logging.debug(ini_list)
    for file_name, section, key, value in INI.read(*ini_list) :
        if section == "Sector" :
           config = Sector
        elif section == "Industry" :
           config = Industry
        elif section == "Category" :
           config = Category
        elif section == "FundFamily" :
           config = FundFamily
        else : continue
        config[key] = value
    return Sector, Industry, Category, FundFamily

def filterByNasdaq(*ini_list) :
    performers = {}
    stability = {}
    Sector = {}
    Industry = {}
    FundFamily = {}
    Category = {}
    
    ini_list = filter(lambda file : "nasdaq_quarterly.ini" in file, ini_list)
    for file_name, section, key, value in INI.read(*ini_list) :
        if section == "Stability" :
           config = stability
        elif section == "Performance" :
           config = performers
        elif section == "Sector" :
           config = Sector
        elif section == "Industry" :
           config = Industry
        elif section == "Category" :
           config = Category
        elif section == "FundFamily" :
           config = FundFamily
        else : continue
        config[key] = value
    ret = performers.get('Q1234',[])
    stock_list = _combineNasdaq(Sector,Industry)
    fund_list = _combineNasdaq(Category,FundFamily)
    ret_stock = sorted(list(set(ret) & set(stock_list)))
    ret_fund = sorted(list(set(ret) & set(fund_list)))
    return stock_list, fund_list, ret_stock, ret_fund 

def _combineNasdaq(left,right) :
    ret = []
    for key in sorted(left.keys()) :
        value = left[key]
        ret += value
    for key in right.keys() :
        value = right[key]
        ret += value
    ret = sorted(list(set(ret)))
    return ret

def findCategoriesOfInterest(**Category) :
    key_list = sorted(Category.keys())
    large_list = filter(lambda key : 'large' in key or 'Large' in key, key_list)
    medium_list = filter(lambda key : ('mid' in key or 'Mid' in key) and not ('small' in key or 'Small' in key), key_list)
    small_list = filter(lambda key : 'small' in key or 'Small' in key, key_list)
    for key in large_list + medium_list + small_list :
        yield key, Category[key]

if __name__ == "__main__" :
   
   import sys
   import pandas as pd
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)
   ini_list = env.list_filenames('local/*.ini')

   Sector, Industry, Category, FundFamily = getByNasdaq(*ini_list)
   for key in Sector.keys() :
       print "Sector", key, Sector[key]
   for key in Industry.keys() :
       print "Industry", key, Industry[key]
   for key, value in findCategoriesOfInterest(**Category) :
       print "Category", key, value

   a,b,c,d = filterByNasdaq(*ini_list) 
   print "NASDAQ Stock count : {}".format(len(a))
   print "NASDAQ Fund count : {}".format(len(b))
   print "Performance Stocks : {}".format(c)
   print "Performance Fund : {}".format(d)
