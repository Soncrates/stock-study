#!/usr/bin/env python

import logging as log
import pandas as PD
from libBusinessLogicMethod05 import FILTER_STOCKS_BY_PERFORNACE,LOAD_HISTORICAL_DATA,STEP_03,STEP_04, process_stock, process_fund
from libBusinessLogicMethod05 import CURATE_BACKGROUND,BACKGROUND, LOAD
from libCommon import find_subset, find_files, LOG_FORMAT_TEST
from libDebug import debug_object, trace
from libDecorators import exit_on_exception, singleton

@singleton
class GGG() :
    var_names = ["cli", "data_store", "background_files", "price_list","ini_list",'floats_in_summary', 'columns_drop','disqualified']

    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*GGG.var_names))
        self.__dict__.update(**self.cli)
        if len(self.suffix) > 0 :
           self.suffix = "_" + self.suffix
        self.entity = self.entity.lower()
        self.flag_stock = 'stock' in self.entity
        self.flag_fund = 'fund' in self.entity
        self.flag_etl = 'etl' in self.entity
        self.flag_etl = False
        if not (self.flag_stock or self.flag_fund or self.flag_etl) :
           raise ValueError('entity must be stocks or funds')
        bg = []
        if self.flag_stock :
           v = filter(lambda x : 'stock' in x, self.background_files)
           bg.extend(list(v))
        if self.flag_fund :
           v = filter(lambda x : 'fund' in x, self.background_files)
           bg.extend(list(v))
        self.background_files = bg
        debug_object(self) 
        
@exit_on_exception
@trace
def main() : 
    step_01 = FILTER_STOCKS_BY_PERFORNACE(GGG().sector_cap,GGG().reduce_risk,GGG().reduce_returns)
    step_02 = LOAD_HISTORICAL_DATA(GGG().price_list,GGG().prices)
    step_03 = STEP_03(GGG().portfolio_iterations,GGG().columns_drop)
    step_04 = STEP_04(GGG().portfolio_iterations,GGG().threshold,GGG().columns_drop)
    reduce_99 = FILTER_STOCKS_BY_PERFORNACE(25,1,2)
    for msg in [step_01,step_02,step_03,step_04] :
        log.info(repr(msg))

    bg = LOAD.background(GGG().background_files)
    bg = PD.DataFrame(bg).T
    bg = CURATE_BACKGROUND.simple(bg, GGG().floats_in_summary, GGG().disqualified)
    bg = CURATE_BACKGROUND.act(bg)
    bg = BACKGROUND.refine(bg)
    bg.drop(['LEN', 'MAX DRAWDOWN','MAX INCREASE'], axis=1,errors='ignore',inplace=True)
    stock, fund = BACKGROUND.by_entity(bg)
    if GGG().flag_stock :
       process_stock(GGG().data_store, GGG().suffix, stock, step_01, step_02, step_03, step_04,reduce_99)
    if GGG().flag_fund :
       process_fund(GGG().data_store,GGG().suffix, fund, step_01, step_02, step_03, step_04)

if __name__ == '__main__' :
   import argparse
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)

   floats_in_summary = ['CAGR','RETURNS','RISK','SHARPE','LEN'] 
   columns_drop = ['returns','risk','sharpe','mean']

   parser = argparse.ArgumentParser(description='Portfoio Generator')
   parser.add_argument('--threshold', action='store', dest='threshold', type=float, default=0.12, help='magic number in refinement')
   parser.add_argument('--iterations', action='store', dest='portfolio_iterations', type=int, default=10000, help='Number of portfolios to try')
   parser.add_argument('--risk', action='store', dest='reduce_risk', type=int, default=7, help='Store a simple value')
   parser.add_argument('--returns', action='store', dest='reduce_returns', type=int, default=1, help='Store a simple value')
   parser.add_argument('--sector', action='store', dest='sector_cap', type=int, default=11, help='Max number of stocks per sector')
   parser.add_argument('--prices', action='store', dest='prices', default='Adj Close', help='Open|Close|Adj Close|Volume')
   parser.add_argument('--suffix', action='store', dest='suffix',default="",help='Store a simple value')
   parser.add_argument('--entity', action='store', dest='entity',default="stock",help='stock|fund')
   cli = vars(parser.parse_args())

   data_store = "{}/outputs".format(env.pwd_parent)
   ini_list = find_files('{}/*.ini'.format(data_store))
   ini_list += find_files('{}/local/*ini'.format(env.pwd_parent))
   price_list = find_files('{}/local/historical_*/*pkl'.format(env.pwd_parent))
   background_files = [ fn for fn in ini_list if 'background.ini' in fn and ('stock_' in fn or 'fund_' in fn) ]
   # too risky AMZN 
   # levelled off after initial increase
   # Not significanly better than SNP
   disqualified = ['TYL','CBPO','COG','CHTR','TPL','TTWO','MNST','VLO','WMB','REGN','GMAB','TCX'
                  , 'NEU','AOS','IFF','PPG','BKNG','STZ','REX','HQL','RGR'
                  ,'FB','WMT','CLX','BOOM','NYT','FNV']

    #keys = ['RISK','SHARPE','CAGR','GROWTH','RETURNS']

   main()

