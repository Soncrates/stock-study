#!/usr/bin/env python

import logging as log
import pandas as PD
from libBusinessLogic import LOAD_HISTORICAL_DATA
from libBusinessLogicMethod05 import FILTER_STOCKS_BY_PERFORNACE,MONTE_CARLO_REFINEMENT_ROUGH,MONTE_CARLO_REFINEMENT_FINE, process_stock, process_fund
from libBusinessLogicMethod05 import CURATE_BACKGROUND,BACKGROUND, LOAD
from libCommon import find_files, LOG_FORMAT_TEST
from libDebug import trace
from libDecorators import exit_on_exception

def businesslogic(**args) :
    log.info(args)
    if len(args['suffix']) > 0 :
       args['suffix'] = "_" + args['suffix']
    args['entity'] = args['entity'].lower()
    args['flag_stock'] = 'stocks' in args['entity'].lower()
    args['flag_fund'] = 'funds' in args['entity'].lower()
    args['flag_etl'] = 'etls' in args['entity'].lower()
    args['flag_etl'] = False
    if not (args['flag_stock'] or args['flag_fund'] or args['flag_etl']) :
       raise ValueError('entity must be stocks or funds')
    bg = []
    if args['flag_stock'] :
       v = filter(lambda x : 'stock' in x, args['background_files'])
       bg.extend(list(v))
    if args['flag_fund'] :
       v = filter(lambda x : 'fund' in x, args['background_files'])
       bg.extend(list(v))
    args['background_files'] = bg
    log.debug(args)
    return args
        
@exit_on_exception
@trace
def main(**args) : 
    args = businesslogic(**args)
    step_01 = FILTER_STOCKS_BY_PERFORNACE(args['sector_cap'],args['reduce_risk'],args['reduce_returns'])
    step_02 = LOAD_HISTORICAL_DATA(args['price_list'],args['prices'])
    step_03 = MONTE_CARLO_REFINEMENT_ROUGH(args['portfolio_iterations'],args['columns_drop'])
    step_04 = MONTE_CARLO_REFINEMENT_FINE(args['portfolio_iterations'],args['threshold'],args['columns_drop'])
    reduce_99 = FILTER_STOCKS_BY_PERFORNACE(25,1,2)
    for msg in [step_01,step_02,MONTE_CARLO_REFINEMENT_ROUGH,step_04] :
        log.info(repr(msg))

    bg = LOAD.background(args['background_files'])
    bg = PD.DataFrame(bg).T
    if args['flag_stock'] :
        drop = [ x for x in bg.columns if x in ['CATEGORY', 'TYPE'] ]
        bg.drop(drop,axis=1,errors='ignore',inplace=True)
    elif args['flag_fund'] :
        drop = [ x for x in bg.columns if x in ['SECTOR'] ]
        bg.drop(drop,axis=1,errors='ignore',inplace=True)
    bg = CURATE_BACKGROUND.simple(bg, args['floats_in_summary'], args['disqualified'])
    bg = CURATE_BACKGROUND.act(bg)
        
    bg = BACKGROUND.refine(bg)
    bg.drop(['LEN', 'MAX DRAWDOWN','MAX INCREASE'], axis=1,errors='ignore',inplace=True)
    stock, fund = BACKGROUND.by_entity(bg)
    if args['flag_stock'] :
       process_stock(args['data_store'], args['suffix'], stock, step_01, step_02, step_03, step_04,reduce_99)
    if args['flag_fund'] :
       process_fund(args['data_store'],args['suffix'], fund, step_01, step_02, step_03, step_04)

if __name__ == '__main__' :
   import argparse
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)

   data_store = "{}/outputs".format(env.pwd_parent)
   ini_list = find_files('{}/*.ini'.format(data_store))
   ini_list += find_files('{}/local/*ini'.format(env.pwd_parent))
   price_list = find_files('{}/local/historical_*/*pkl'.format(env.pwd_parent))
   background_files = [ fn for fn in ini_list if 'background.ini' in fn and ('stock_' in fn or 'fund_' in fn) ]

   parser = argparse.ArgumentParser(description='Portfoio Generator')
   parser.add_argument('--threshold', action='store', dest='threshold', type=float, default=0.12, help='magic number in refinement')
   parser.add_argument('--iterations', action='store', dest='portfolio_iterations', type=int, default=10000, help='Number of portfolios to try')
   parser.add_argument('--risk', action='store', dest='reduce_risk', type=int, default=7, help='Store a simple value')
   parser.add_argument('--returns', action='store', dest='reduce_returns', type=int, default=1, help='Store a simple value')
   parser.add_argument('--sector', action='store', dest='sector_cap', type=int, default=11, help='Max number of stocks per sector')
   parser.add_argument('--prices', action='store', dest='prices', default='Adj Close', help='Open|Close|Adj Close|Volume')
   parser.add_argument('--suffix', action='store', dest='suffix',default="",help='Store a simple value')
   parser.add_argument('--entity', action='store', dest='entity',default="stocks",help='stocks|funds|etls')

   # too risky AMZN 
   # levelled off after initial increase
   # Not significanly better than SNP
   disqualified = ['TYL','CBPO','COG','CHTR','TPL','TTWO','MNST','VLO','WMB','REGN','GMAB','TCX'
                  , 'NEU','AOS','IFF','PPG','BKNG','STZ','REX','HQL','RGR'
                  ,'FB','WMT','CLX','BOOM','NYT','FNV']

   floats_in_summary = ['CAGR','RETURNS','RISK','SHARPE','LEN'] 
   columns_drop = ['returns','risk','sharpe','mean']

   var_names = ["data_store", "background_files", "price_list","ini_list",'floats_in_summary', 'columns_drop','disqualified']
   init = { key : value for (key,value) in globals().items() if key in var_names }
   init.update(**vars(parser.parse_args()))
   main(**init)


