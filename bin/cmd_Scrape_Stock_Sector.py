#!/usr/bin/env python

import logging as log
from libBusinessLogic import NASDAQ_EXTRACT
from libBusinessLogicStockSector import STOCKMONITOR,EXTRACT_SECTOR,TRANSFORM_SECTOR
from libCommon import INI_READ, INI_WRITE, find_subset, LOG_FORMAT_TEST
from libCommon import dict_append_list
from libDecorators import exit_on_exception, singleton
'''
   Web Scraper
   Use RESTful interface to to get web pages and parse for relevant info about stocks and funds
'''

@singleton
class VARIABLES() :
    var_names = ['env','draft','final','omit_list','save_file','sector_enum','headers', 'stock_names', 'alias']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*VARIABLES.var_names))

class TRANSFORM() :
      @classmethod
      def merge(cls,omit_list, *file_list) :
          ret = {}
          for section, key, stock in INI_READ.read(*file_list) :
              if isinstance(stock,str) :
                  stock = [stock]
              dict_append_list(ret, key,*stock)
          ret = { key : value for (key,value) in ret.items() if key not in omit_list }
          return ret

@exit_on_exception
def main() :
    draft = EXTRACT_SECTOR(VARIABLES().stock_names,VARIABLES().alias,VARIABLES().sector_enum,VARIABLES().headers)
    #alias = draft.pop('alias',{})
    data = { key : sorted(value) for (key,value) in draft.items() }
    data.update(VARIABLES().alias)
    INI_WRITE.write(format(VARIABLES().draft), **data)

    final = TRANSFORM.merge(VARIABLES().omit_list,*[VARIABLES().draft])

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
   sector_enum = TRANSFORM_SECTOR.sector_set
   headers = STOCKMONITOR.default_headers
   ticker_list = NASDAQ_EXTRACT()
   stock_names = ticker_list['stock_list']
   alias       = ticker_list['alias_list']
   main()
