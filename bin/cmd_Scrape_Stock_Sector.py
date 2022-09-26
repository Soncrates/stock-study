#!/usr/bin/env python

import logging as log

from libBusinessLogic import NASDAQ_EXTRACT
from libBusinessLogic import INI_READ,INI_WRITE
from libBusinessLogicStockSector import STOCKMONITOR,EXTRACT_SECTOR,TRANSFORM_SECTOR
from libCommon import LOG_FORMAT_TEST,dict_append_list
from libDecorators import exit_on_exception
'''
   Web Scraper
   Use RESTful interface to to get web pages and parse for relevant info about stocks and funds
'''

def merge(omit_list, *file_list) :
    ret = {}
    for section, key, stock in INI_READ.read(*file_list) :
        if isinstance(stock,str) :
            stock = [stock]
        dict_append_list(ret, key,*stock)
    ret = { key : value for (key,value) in ret.items() if key not in omit_list }
    return ret

@exit_on_exception
def main(**args) :
    log.debug(args)
    ticker_list = NASDAQ_EXTRACT()
    log.info(ticker_list['alias_list'])

    draft = EXTRACT_SECTOR(ticker_list['stock_list'],ticker_list['alias_list'],args['sector_enum'],args['headers'])
    INI_WRITE.write(args['draft'], **draft)

    final = merge(args['omit_list'],*[args['draft']])
    INI_WRITE.write(args['output_file'],**{'MERGED' : final})

if __name__ == '__main__' :
   import argparse
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)

   draft = '{}/outputs/stock_by_sector_draft.ini'.format(env.pwd_parent)
   output_file = '{}/outputs/stock_by_sector.ini'.format(env.pwd_parent)
   omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']
   
   parser = argparse.ArgumentParser(description='Scrape Sector data for Stock')
   parser.add_argument('--draft', action='store', dest='draft', type=str, default=draft, help='temporary file')
   parser.add_argument('--output', action='store', dest='output_file', type=str, default=output_file, help='store report meta')
   
   sector_enum = TRANSFORM_SECTOR.sector_set
   headers = STOCKMONITOR.default_headers
   
   var_names = ['env','draft','output_file','omit_list','sector_enum','headers']
   init = { key : value for (key,value) in globals().items() if key in var_names }
   init.update(**vars(parser.parse_args()))
   main(**init)
