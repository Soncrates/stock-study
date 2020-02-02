#!/usr/bin/env python

import logging
from libCommon import INI, ENVIRONMENT, exit_on_exception, log_on_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libFinance import TRANSFORM_SHARPE as SHARPE, TRANSFORM_CAGR as CAGR
from cmd_Scrape_Stock_Sector import DICT_HELPER
from libDebug import trace

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, data_store, config_file,output_file, reader) :
        self.env = _env
        self.data_store = data_store
        self.config_file = config_file
        self.output_file = output_file
        self.reader = reader
        self.benchmarks = ['Index','MOTLEYFOOL','PERSONAL']
        self.benchmarks = ['Index']
        self.omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']
        msg = vars(self)
        for i, key in enumerate(sorted(msg)) :
            value = msg[key]
            if isinstance(value,list) and len(value) > 10 :
               value = value[:10]
            logging.info((i,key, value))
    @classmethod
    def instance(cls, **kwargs) :
        if not (cls._singleton is None) :
           return cls._singleton
        target = 'env'
        _env = globals().get(target,None)
        target = "data_store"
        data_store = globals().get(target,[])
        target = "output_file"
        output_file = globals().get(target,[])
        target = 'config_file'
        config_file = globals().get(target,[])
        if not isinstance(config_file,list) :
           config_file = list(config_file)
        if len(_env.argv) > 1 :
           config_file = [_env.argv[1]]
        if len(_env.argv) > 2 :
           output_file = [_env.argv[2]]
        reader = STOCK_TIMESERIES.init()
        env.mkdir(data_store)
        cls._singleton = cls(_env,data_store,config_file,output_file,reader)
        return cls._singleton
    @classmethod
    def config(cls) :
        config = cls.instance().config_file
        logging.info("loading results {}".format(config))
        for path, section, key, stock_list in INI.loadList(*config) :
            yield path, section, key, stock_list
    @classmethod
    def benchmarks(cls) :
        ret = DICT_HELPER.init()
        for path, section, key, stock in cls.config() :
            if section not in cls.instance().benchmarks :
                continue
            logging.info((section,key,stock))
            ret.append(key,*stock)
        for omit in cls.instance().omit_list :
            ret.data.pop(omit,None)
        logging.info(ret)
        stock_list = ret.values()
        return ret.data, stock_list
    @classmethod
    @log_on_exception
    def prices(cls, data_store, ticker) :
        filename = '{}/{}.pkl'.format(data_store,ticker)
        name, data = STOCK_TIMESERIES.load(filename)
        return data

class TRANSFORM() :
    _prices = 'Adj Close'
    @classmethod
    def _data(cls, value) :
        if isinstance(value,list) :
           value = value[0]
        if isinstance(value,str) :
           return value
        return 'Unknown'
    @classmethod
    def data(cls, data) :
        key_list = sorted(data.keys())
        value_list = map(lambda key : data[key], key_list)
        value_list = map(lambda key : cls._data(key), value_list)
        ret = dict(zip(value_list,key_list))
        return ret

    @classmethod
    def prices(cls, data) :
          cagr = 0
          stdev = 0
          _len = 0
          sharpe = 0
          growth = 0
          if data is None :
             return cagr, stdev, _len, sharpe, growth
          prices = data[cls._prices]
          cagr, growth = CAGR.find(prices)
          ret = SHARPE.find(prices, period=FINANCE.YEAR, span=0)
          target = 'risk'
          stdev = ret.get(target,stdev)
          target = 'sharpe'
          sharpe = ret.get(target,sharpe)
          target = 'len'
          _len = ret.get(target,_len)
          return cagr, stdev, _len, sharpe, growth

class LOAD() :
      @classmethod
      def _prices(cls, local_dir, ticker,dud) :
          filename = '{}/{}.pkl'.format(local_dir,ticker)
          reader =  EXTRACT.instance().reader
          prices = reader.extract_from_yahoo(ticker)
          if prices is None :
             dud.append(ticker)
             return dud
          STOCK_TIMESERIES.save(filename, ticker, prices)
          return dud
      @classmethod
      def prices(cls,local_dir, ticker_list) :
          dud = []
          for ticker in ticker_list :
              dud = cls._prices(local_dir, ticker,dud)
          size = len(ticker_list) - len(dud)
          logging.info("Total {}".format(size))
          if len(dud) > 0 :
             dud = sorted(dud)
             logging.warn((len(dud),dud))
          return dud
      @classmethod
      @trace
      def robust(cls,data_store, ticker_list) :
          retry = cls.prices(data_store, ticker_list)
          if len(retry) > 0 :
             retry = cls.prices(data_store, retry)
          if len(retry) > 0 :
             logging.error((len(retry), sorted(retry)))
      @classmethod
      def background(cls, **config) :
          save_file = EXTRACT.instance().output_file
          ret = INI.init()
          for key in sorted(config) :
              value = config.get(key,[])
              INI.write_section(ret,key,**value)
          ret.write(open(save_file, 'w'))
          logging.info("results saved to {}".format(save_file))

def process_prices(ticker_list) :
    data_store = EXTRACT.instance().data_store
    cagr_list = []
    stdev_list = []
    len_list = []
    sharpe_list = []
    growth_list = []
    for ticker in ticker_list :
        prices = EXTRACT.prices(data_store, ticker)
        cagr, stdev, _len, sharpe, growth = TRANSFORM.prices(prices)
        cagr_list.append(cagr)
        stdev_list.append(stdev)
        len_list.append(_len)
        sharpe_list.append(sharpe)
        growth_list.append(growth)
    cagr = dict(zip(ticker_list,cagr_list))
    stdev = dict(zip(ticker_list,stdev_list))
    sharpe = dict(zip(ticker_list,sharpe_list))
    _len = dict(zip(ticker_list,len_list))
    growth = dict(zip(ticker_list,growth_list))
    ret = { 'CAGR' : cagr, 'RISK' : stdev, 'SHARPE' : sharpe, 'LEN' : _len, 'GROWTH' : growth }
    return ret

@exit_on_exception
@trace
def main() : 
    data, stock_list = EXTRACT.benchmarks()
    data_store = EXTRACT.instance().data_store
    LOAD.robust(data_store, stock_list)
    background = process_prices(stock_list)
    names = TRANSFORM.data(data)
    background['NAME'] = names
    LOAD.background(**background)

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '{}/historical_prices'.format(local_dir)
   data_store = '../local/historical_prices'

   ini_list = env.list_filenames('local/*.ini')
   config_file = filter(lambda x : 'benchmark' in x, ini_list)
   output_file = "{}/benchmark_background.ini".format(local_dir)

   main()
