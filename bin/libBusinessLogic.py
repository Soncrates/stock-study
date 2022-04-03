# -*- coding: utf-8 -*-
from copy import deepcopy
import datetime
import logging as log
"""
Created on Fri Apr  1 13:59:31 2022

@author: emers
"""

class YAHOO_SCRAPER:
      '''
      Interface into pandas DataReader
      '''
      source = 'yahoo'
      size = 365*10
      date_format = '%Y-%m-%d'
      @classmethod
      def pandas(cls, **kwargs) :
          target = 'end'
          end = kwargs.get(target, datetime.datetime.utcnow())
          if isinstance(end, str) :
             end = datetime.datetime.strptime(end, cls.date_format)
          target = 'start'         
          start = kwargs.get(target, datetime.timedelta(days=cls.size))
          if isinstance(start, str) :
             start = int(start)
          start = end - start
          ret = { 'start':start, 'end':end, 'source':cls.source}
          log.info(ret)
          return ret
      @classmethod
      def make_args(cls, *ticker_list,**kwargs) :
          '''
          Yields
          ------
          args : counter, dict
              parameters for pandas DataReader.
              
          WARNING: del from calling function!!!!!
          '''
          total = len(ticker_list)
          for i, ticker in enumerate(ticker_list) :
              args = deepcopy(kwargs)
              args['ticker'] = ticker
              log.info("{} ({}/{})".format(ticker,i,total))
              yield i, args
      