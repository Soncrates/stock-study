#!/usr/bin/python

import logging
import sys
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libDecorators import log_on_exception, singleton
from libDebug import trace
from libDebug import debug_object
import cmd_Scrape_Stock_Sector as TEST

def get_globals(*largs) :
    logging.info("Valid globals : {}".format(sorted(globals().keys())))
    ret = {}
    for name in largs :
        value = globals().get(name,None)
        if value is None :
           logging.warning("Not a global : {}".format(name))
           continue
        ret[name] = value
    return ret

@singleton
class SECTOR_T() :
    var_names = ['ini_files','file_list','DMYT','sector_enum']
    def __init__(self) :
        values = get_globals(*SECTOR_T.var_names)
        self.__dict__.update(**values)
        debug_object(self)

class TemplateTest(unittest.TestCase):

    #@unittest.skip("demonstrating skipping")
    def test_03_(self) :
        a, b = TEST.YAHOO.extract(SECTOR_T().DMYT,SECTOR_T().sector_enum)
        logging.info(a)
        logging.info(b)
         

if __name__ == '__main__' :

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   DMYT = ['DMYT','DMYT+', 'DMYT=','_','DOOR','ZEUS','CHTR','LILA','HOME','TOUR','COKE','PEP','BOOM','FANG','ETON','ZYNE','AQUA','GASS','BXMT','GOOD','AAPL','LEAF','SONG','QUOTE']
   sector_enum = ['Basic Materials', 'Consumer Defensive', 'Consumer Cyclical'
                 ,'Communication Services', 'Energy', 'Financial Services'
                 ,'Healthcare','Industrials','Real Estate'
                 ,'Utilities','Technology'
                 ]


   unittest.main()
