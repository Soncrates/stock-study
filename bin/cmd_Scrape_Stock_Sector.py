#!/usr/bin/env python

import logging as log
from functools import reduce
from libBusinessLogic import NASDAQ_EXTRACT
from libBusinessLogicStockSector import STOCKMONITOR,EXTRACT_SECTOR
from libCommon import INI_READ, INI_WRITE, find_subset, LOG_FORMAT_TEST
from libCommon import dict_append_list
from libDecorators import exit_on_exception, singleton
from libDebug import trace
'''
   Web Scraper
   Use RESTful interface to to get web pages and parse for relevant info about stocks and funds
'''

@singleton
class VARIABLES() :
    var_names = ['env','draft','final','omit_list','save_file','sector_enum','headers', 'stock_names', 'alias']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*VARIABLES.var_names))

def handle_alias_retry(ret,alias) :
    retry = [ list(set(alias.get(x,[]))) for x in ret ]
    if len(retry) > 0 :
       retry = reduce(lambda a, b : a+b, retry)
    return list(retry)

def handle_alias(*stock_list,**alias) :
    ret = set(alias.keys()).intersection(set(stock_list))
    left_overs = set(stock_list) - ret
    ret = sorted(list(ret))
    left_overs = sorted(list(left_overs))
    retry = handle_alias_retry(ret,alias)
    log.info(ret)
    log.debug(retry)
    log.debug(left_overs)
    return ret, retry, left_overs 

class TRANSFORM() :
      @classmethod
      def merge(cls) :
          ret = {}
          for section, key, stock in INI_READ.read(*[VARIABLES().draft]) :
              dict_append_list(ret, key,*stock)
          for omit in VARIABLES().omit_list :
              ret.pop(omit,None)

          stock_list = list(ret.values())
          return ret, stock_list

@exit_on_exception
@trace
def main() :
    draft = EXTRACT_SECTOR(VARIABLES().stock_names,VARIABLES().alias,VARIABLES().sector_enum,VARIABLES().headers)
    #alias = draft.pop('alias',{})
    data = { key : sorted(value) for (key,value) in draft.items() }
    data.update(alias)
    INI_WRITE.write(format(VARIABLES().draft), **data)
    final, stock_list = TRANSFORM.merge()
    INI_WRITE.write(VARIABLES().final,**{'MERGED' : final})

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)

   draft = '{}/local/stock_by_sector_draft.ini'.format(env.pwd_parent)
   final = '{}/local/stock_by_sector.ini'.format(env.pwd_parent)
   save_file = '{}/local/stock_background.ini'.format(env.pwd_parent)
   omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']
   sector_enum = EXTRACT_SECTOR.sector_set
   headers = STOCKMONITOR.default_headers
   ticker_list = NASDAQ_EXTRACT()
   stock_names = ticker_list['stock_list']
   alias       = ticker_list['alias_list']
   main()
