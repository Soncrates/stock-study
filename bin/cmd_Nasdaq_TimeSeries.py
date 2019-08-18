#!/usr/bin/python

import datetime
import logging

from libCommon import STOCK_TIMESERIES, NASDAQ

'''
   Web Scraper
   Use RESTful interface to download stock and fund stock market prices for the past 10 years
   Store as pkl files
'''
class Refresh :
      def __init__(self,end) :
             self.end = end
      def __call__(self,epoch) :
          flag_seconds = self._epoch_now() - epoch
          flag_hour = flag_seconds / 3600
          flag_day = flag_hour / 24
          flag_month = flag_day / 30
          return int(flag_month) > 1
      def _epoch_now(self) :
          ret = datetime.datetime(1970,1,1)
          ret = self.end - ret
          return ret.total_seconds()

def main(pwd, *file_list) :
    reader = STOCK_TIMESERIES.init()
    refresh = Refresh(reader.end)
    nasdaq = NASDAQ.init()

    fresh_list = filter(lambda path : not refresh(os.path.getmtime(path)) , file_list)

    for stock in nasdaq() :
        filename = '{}/historical_prices/{}.pkl'.format(pwd,stock)
        #if filename in fresh_list : continue
        ret = reader.extract_from_yahoo(stock)
        STOCK_TIMESERIES.save(filename, stock, ret)

if __name__ == "__main__" :
   from glob import glob
   import os,sys
   from libCommon import TIMER

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   pwd = pwd.replace('bin','local')
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))

   logging.info("started {}".format(name))
   elapsed = TIMER.init()
   main(pwd,*file_list)
   logging.info("finished {} elapsed time : {} ".format(name,elapsed()))

