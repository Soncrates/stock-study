#!/usr/bin/python

import datetime

from libCommon import STOCK_TIMESERIES, NASDAQ

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
        if filename in fresh_list : continue
        ret = reader.extract_from_yahoo(stock,filename)
        STOCK_TIMESERIES.save(filename, stock, ret)

if __name__ == "__main__" :
   from glob import glob
   import os,sys

   pwd = os.getcwd()
   pwd = pwd.replace('bin','lib')
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))
   main(pwd,*file_list)
