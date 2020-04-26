#!/usr/bin/python

import logging
import sys
import re
import time
import unittest
from collections import Counter
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libNLP import WORDS
from libUtils import log_on_exception, RSS, WEB
from libDebug import trace

class RSS_REFORMED_BROKER :
      rss = "https://thereformedbroker.com/feed/"
      fields = ['title', 'link', 'published','published_parsed','authors','tags','summary','summary_detail','comments']
      @classmethod
      def news(cls) :
          for entry in RSS.to_dict(cls.rss):
              if isinstance(entry,str) :
                 continue
              entry = RSS.narrow(*cls.fields,**entry)
              for key in sorted(entry) :
                  logging.info((key,entry[key]))
class RSS_REFORMED_BROKER :
      rss = 'https://investorjunkie.com/feed '
      fields = ['title', 'link', 'published','published_parsed','authors','tags','summary','summary_detail','comments']
      @classmethod
      def news(cls) :
          for entry in RSS.to_dict(cls.rss):
              if isinstance(entry,str) :
                 continue
              entry = RSS.narrow(*cls.fields,**entry)
              for key in sorted(entry) :
                  logging.info((key,entry[key]))
class RSS_REFORMED_BROKER :
      rss = 'https://www.whitecoatinvestor.com/feed/'
      fields = ['title', 'link', 'published','published_parsed','authors','tags','summary','summary_detail','comments']
      @classmethod
      def news(cls) :
          for entry in RSS.to_dict(cls.rss):
              if isinstance(entry,str) :
                 continue
              entry = RSS.narrow(*cls.fields,**entry)
              for key in sorted(entry) :
                  logging.info((key,entry[key]))
class RSS_REFORMED_BROKER :
      rss = 'https://investingnews.com/feed/'
      fields = ['title', 'link', 'published','published_parsed','authors','tags','summary','summary_detail','comments']
      @classmethod
      def news(cls) :
          for entry in RSS.to_dict(cls.rss):
              if isinstance(entry,str) :
                 continue
              entry = RSS.narrow(*cls.fields,**entry)
              for key in sorted(entry) :
                  logging.info((key,entry[key]))
class RSS_REFORMED_BROKER :
      rss = 'https://www.investorideas.com/RSS/feeds/IIMAIN.xml'
      fields = ['title', 'link', 'published','published_parsed','authors','tags','summary','summary_detail','comments']
      fields = ['title', 'link' ]
      @classmethod
      def news(cls) :
          for entry in RSS.to_dict(cls.rss):
              if isinstance(entry,str) :
                 continue
              entry = RSS.narrow(*cls.fields,**entry)
              for key in sorted(entry) :
                  logging.info((key,entry[key]))
class RSS_REFORMED_BROKER :
      rss = 'https://www.financebrokerage.com/feed/'
      fields = ['comments','content','link','published','published_parsed','summary','summary_detail','title']
      @classmethod
      def news(cls) :
          for entry in RSS.to_dict(cls.rss):
              if isinstance(entry,str) :
                 continue
              entry = RSS.narrow(*cls.fields,**entry)
              for key in sorted(entry) :
                  value = entry[key]
                  if key not in ['content','summary','summary_detail'] :
                     logging.info((key,value))
                     continue
                  if isinstance(value,str) :
                     value = '<html><body>{}</body></html>'.format(value)
                  elif isinstance(value,dict):
                       value = '<html><body>{value}</body></html>'.format(**value)
                  else :
                       target = 'value'
                       value = map(lambda v : v.get(target,''), value)
                       value = "".join(list(value))
                  value = WEB.format_as_soup(value)
                  sentences = value.find_all(text=True)
                  value = WORDS.tokenize(sentences)
                  logging.info((key,value))
class RSS_NASDAQ :
      rss = 'https://www.nasdaq.com/feed/rssoutbound?symbol={}'
      key_01 = 'nasdaq_tickers'
      key_02 = 'link'
      key_03 = 'summary'
      fields = ['title', key_02, 'published','published_parsed',key_03,'authors','tags',key_01]
      old = 68000000
      @classmethod
      def is_old(cls,entry) :
          if isinstance(entry,str) :
             return False
          ret = time.time() - time.mktime(entry.published_parsed)
          return ret > cls.old
      @classmethod
      def transform(cls,entry) :
          value_list = sorted(entry[cls.key_01].split(','))
          value_list = map(lambda x : x.strip(), value_list)
          entry[cls.key_01] = list(value_list)
          url = entry[cls.key_03]
          logging.info(url)
          url = word_extraction(url)
          logging.info(url)
          #response = WEB.get_text(url)
          #logging.info(response)
          return entry
      @classmethod
      def flag_skip(cls,entry) :
          if isinstance(entry,str) :
             return False, []
          if cls.is_old(entry) :
             return True, []
          e = entry.get(cls.key_01,'NA')
          if e == 'NA' :
             return True, []
          return False, e.split(',')
      @classmethod
      def news(cls,ticker) :
          rss = cls.rss.format(ticker)
          for entry in RSS.to_dict(rss):
              flag_skip, ticker_list = cls.flag_skip(entry)
              if flag_skip :
                 continue
              if ticker not in ticker_list :
                 continue
              ret = RSS.narrow(*cls.fields,**entry)
              ret = cls.transform(ret)
              yield ret

def generate_bow(allsentences):
    vocab = tokenize(allsentences)
    print("Word List for Document \n{0} \n".format(vocab));
    for sentence in allsentences:
        words = word_extraction(sentence)
        bag_vector = numpy.zeros(len(vocab))
        for w in words:
            for i,word in enumerate(vocab):
                if word == w:
                    bag_vector[i] += 1
                    print("{0}\n{1}\n".format(sentence,numpy.array(bag_vector)))
    
class TemplateTest(unittest.TestCase):

    def test_04_(self) :
        RSS_REFORMED_BROKER.news()
    def dep_test_03_(self) :
        target = 'stock_list'
        stock_list = globals().get(target,[])
        ret = {}
        for i, stock in enumerate(sorted(stock_list)) :
            ret[stock] = Counter()
            for news in RSS_NASDAQ.news(stock) :
                if isinstance(news,str) :
                   logging.info(news)
                   continue
                #for key in sorted(news) :
                #    logging.info((stock,key,news[key]))
                _list = news[RSS_NASDAQ.key_01]
                for _ticker in _list :
                    ret[stock][_ticker] += 1
        omit = filter(lambda stock : len(ret[stock]) == 0, ret)
        omit = list(omit)
        for key in omit :
            ret.pop(key)

        for stock in sorted(ret) :
            logging.info((stock,ret[stock]))

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   ini_list = env.list_filenames('local/*ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   stock_list = ['AAPL','GOOG','SCHA','SPY', 'SRCpA','SRC-A', 'SRC$A', 'SRCA']

   unittest.main()
   #main()
   

