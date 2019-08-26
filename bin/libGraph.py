#!/usr/bin/python

import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class LINE :
      @staticmethod
      def plot(lines) :
          for data, label in LINE._xy(lines)
              data.plot(label=label)
      @staticmethod
      def _xy(data) :
          for key in sorted(data.keys()) :
              yield data[key], key
class BAR :
      @staticmethod
      def plot(bar) :
          label, data, pos = BAR._xy(bar)
          plt.barh(pos, data, align='center', alpha=0.5)
          plt.yticks(pos, label)
      @staticmethod
      def _xy(data) :
          label_list = data.keys()
          data_list = data.values()
          y_pos = np.arange(len(label_list))
          return label_list, data_list, y_pos

class POINT :
      @staticmethod
      def plot(points, _x, _y) :
          for x, y, label in POINT._xy(points,_x,_y) :
              #plt.scatter(x,y)
              #ax = point.plot(x='x', y='y', ax=ax, style='bx', label='point')
              plt.plot(x,y,'o', label=label)
      @staticmethod
      def _xy(data,x,y) :
          for key in sorted(data.keys()) :
              pt = data[key]
              yield pt[x], pt[y]i, key

def save(path) :
    plt.legend()
    plt.savefig(path)
    plt.clf()
    plt.cla()
    plt.close()

if __name__ == '__main__' :

   from glob import glob
   import os,sys
   from libCommon import TIMER

   pwd = os.getcwd()
   local = pwd.replace('bin','local')
   portfolio_ini = glob('{}/yahoo_sharpe_method1*portfolios.ini'.format(local))
   ini_list = glob('{}/*.ini'.format(local))
   file_list = glob('{}/historical_prices/*pkl'.format(local))

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   logging.info("started {}".format(name))
   elapsed = TIMER.init()
   logging.info("finished {} elapsed time : {} ".format(name,elapsed()))
