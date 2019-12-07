#!/usr/bin/env python

import re
import logging
from libCommon import INI, log_exception
from libDebug import trace
from libWeb import WEB_UTIL

'''
   Web Scraper
   Use RESTful interface to to get web pages and parse for relevant info about stocks and funds
'''
class SECTOR() :
      url = 'https://www.stockmonitor.com/sector/{}'
      url_list = [
      'basic-materials'
      , 'communication-services'
      , 'consumer-cyclical'
      , 'consumer-defensive'
      , 'energy'
      , 'financial-services'
      , 'healthcare'
      , 'industrials'
      , 'technology'
      , 'utilities'
      ]
      def __init__(self, url_list) :
          self.url_list = url_list
      def __str__(self) :
          msg = self.url_list.values()
          msg = ','.join(sorted(msg))
          return msg
      def __call__(self) :
          ret = {}
          total = 0
          for sector in sorted(self.url_list) : 
              logging.debug(sector)
              url = self.url_list[sector]
              stock_list = SECTOR.get(url)
              logging.info((sector,len(stock_list)))
              ret[sector] = stock_list
              total += len(stock_list)
          logging.info(("Total",total))
          return ret

      @classmethod
      def init(cls) :
          key_list = map(lambda x : x.replace('-','_'), cls.url_list)
          url_list = map(lambda x : cls.url.format(x), cls.url_list)
          url_list = dict(zip(key_list,url_list))
          ret = cls(url_list)
          return ret
      @classmethod
      def get(cls,url) :
          response = WEB_UTIL.invoke_url(url)
          soup = WEB_UTIL.format_as_soup(response)
          ret = set([])
          for link in soup.findAll('a', attrs={'href': re.compile("^/quote")}):
              ret.add( link.contents[0] )
          return sorted(list(ret))

@log_exception
@trace
def main(save_file) :
    obj = SECTOR.init()
    logging.info(obj)
    ret = obj()
    config = INI.init()
    INI.write_section(config,"SECTOR",**ret)
    config.write(open(save_file, 'w'))

if __name__ == '__main__' :

   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   stock_ini = '{}/local/stockmonitor_background.ini'.format(env.pwd_parent)
   main(stock_ini)

