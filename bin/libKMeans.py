#!/usr/bin/env python

import logging
import pandas as pd

from sklearn.cluster import KMeans
import  pylab as pl
import numpy as np

from libCommon import INI
from libUtils import combinations, exit_on_exception, log_on_exception

class EXTRACT_K():
    @classmethod
    def test(cls,ret) :
        X =  ret.values #Converting ret_var into nummpy arraysse = []for k in range(2,15):
        #test for number of categories
        #kmeans = KMeans(n_clusters = k)
        #kmeans.fit(X)
        #sse.append(kmeans.inertia_) #SSE for each n_clusters
        #centroids = kmeans.cluster_centers_
    @classmethod
    def dynamic_cluster(cls,ret) :
        X =  ret.values 
        groups = int(len(X)/10)
        if groups < 5 :
           groups = 5
        if groups > len(X) :
           groups = len(X)-1
        logging.debug(groups)
        return cls.cluster(ret,groups)
    @classmethod
    def cluster(cls,data,groups) :
        X =  data.values 
        kmeans = KMeans(n_clusters = groups).fit(X)
        ret = pd.DataFrame(kmeans.labels_)
        ret.rename(columns={0:'K'},inplace=True)
        _x = range(0,len(ret.index.values))
        ticker = dict(zip(_x,data.index.values))
        ret.rename(index=ticker,inplace=True)
        return ret
    @classmethod
    def enumerate(cls,ret) :
        _list = ret['K']
        _list = sorted(set(_list))
        for entry in _list :
            group = ret[ret['K'] == entry]
            logging.info((entry,len(group), sorted(group.index.values)))
            yield entry, group

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   local_dir = "{}/local".format(env.pwd_parent)
   ini_list = env.list_filenames('local/*.ini')
   sector = filter(lambda x : 'stock_by_sector.ini' in x , ini_list)
   background = filter(lambda x : 'background.ini' in x, ini_list)
   background = filter(lambda x : 'stock_' in x or 'fund_' in x, background)
   benchmark = filter(lambda x : 'benchmark' in x, ini_list)
   file_list = env.list_filenames('local/historical_*/*pkl')

   main()
   # Execution speed for main : hours : 4.0, minutes : 7.0, seconds : 48.5
