#!/usr/bin/python

import logging
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libCommon import find_subset, LOG_FORMAT_TEST, iterate_config,INI_READ,find_files
from libDecorators import singleton
from libBusinessLogicStockSector import TRANSFORM_SECTOR as TEST_01, YAHOO as TEST_02
from libBusinessLogicStockSector import FINANCEMODELLING_STOCK_LIST as TEST_03, FINANCEMODELLING as TEST_04
from libBusinessLogicStockSector import STOCKMONITOR as TEST_05, EXTRACT_SECTOR as FINAL

@singleton
class TTT() :
    names = ['ini_files','file_list','stock_list','sector_enum','alias_list', 'headers']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*TTT.names))

class TEST_SECTOR(unittest.TestCase):

    #@unittest.skip("demonstrating skipping")
    def test_01(self) :
        a= TEST_01.normalize('Estate')
        self.assertEqual(a, 'Real Estate')
        a= TEST_01.normalize('BASIC')
        self.assertEqual(a, 'Basic Materials')
        a= TEST_01.normalize('utilities')
        self.assertEqual(a, 'Utilities')
        a= TEST_01.normalize('Communication')
        self.assertEqual(a, 'Communication Services')
        a= TEST_01.normalize('Defensive')
        self.assertEqual(a, 'Consumer Defensive')
        a= TEST_01.normalize('Cyclical')
        self.assertEqual(a, 'Consumer Cyclical')
        a= TEST_01.normalize('ENERGY')
        self.assertEqual(a, 'Energy')
        a= TEST_01.normalize('technology')
        self.assertEqual(a, 'Technology')
        a= TEST_01.normalize('healthcare')
        self.assertEqual(a, 'Healthcare')
        a= TEST_01.normalize('industrials')
        self.assertEqual(a, 'Industrials')
        a= TEST_01.normalize('financial')
        self.assertEqual(a, 'Financial Services')
        a= TEST_01.normalize('Industrial Goods')
        self.assertEqual(a, 'Industrial Goods')
        a= TEST_01.normalize('Unknown')
        self.assertEqual(a, 'Unknown')

    #@unittest.skip("demonstrating skipping")
    def test_02(self) :
        a, b = TEST_02.extract(TTT().stock_list,TTT().sector_enum)
        logging.info(a)
        logging.info(b)
        self.assertEqual('Consumer Defensive' in a, True)
        self.assertEqual('Technology' in a, True)
        self.assertEqual('AAPL' in b, True)
        self.assertEqual('COKE' in b, True)
        self.assertEqual('Technology' in a, True)
         
    @unittest.skip("FINANCEMODELLING_STOCK_LIST calls https://financialmodelingprep.com/api/v3/company/stock/list, which is now forbidden")
    def test_03(self) :
        a = TEST_03.get()
        logging.info(a)

    @unittest.skip("FINANCEMODELLING_STOCK_LIST calls https://financialmodelingprep.com/api/v3/company/profile/, which now charges fees")
    def test_04(self) :
        a, b = TEST_04.extract(TTT().stock_list)
        logging.info(a)
        logging.info(b)

    #@unittest.skip("demonstrating skipping")
    def test_05(self) :
        a, b = TEST_05.extract(TTT().headers)
        logging.info(a)
        logging.info(b)

    #@unittest.skip("demonstrating skipping")
    def test_Final(self) :
        draft = FINAL(TTT().stock_list,TTT().alias_list,TTT().sector_enum,TTT().headers)
        for i,j, section, key, value in iterate_config(draft) :
            logging.info(value)
            logging.info((type(value)))
            logging.info((section,key))
        #alias = draft.pop('alias',{})
        #data = { key : sorted(value) for (key,value) in draft.items() }
        #logging.debug(data)
        #for key in data :
        #    logging.info((key,data[key]))
        #data.update(alias)
        #LOAD.draft(draft)
        #final, stock_list = TRANSFORM.merge()
        #LOAD.final(final)

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   logging.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=logging.DEBUG)
   
   stock_list = ['DMYT','DMYT+', 'DMYT=','_','DOOR','ZEUS','CHTR','LILA','HOME','TOUR','COKE','PEP','BOOM','FANG','ETON','ZYNE','AQUA','GASS','BXMT','GOOD','AAPL','LEAF','SONG','QUOTE']
   stock_list = ['DMYT','DMYT+', 'DMYT=','DOOR','ZEUS','CHTR','LILA','HOME','COKE','PEP','GOOD','AAPL','LEAF','SONG','QUOTE']
   #stock_list = ['COKE','PEP','AAPL','QUOTE']
   alias_list = ['DMYT','DMYT+', 'DMYT=']
   sector_enum = TEST_01.sector_set
   headers = TEST_05.default_headers
   
   
   '''
   Stock (9281) ['AACG', 'AACI', 'AACIU', 'AACIW', 'AADI', 'AAL', 'AAME', 'AAOI', 'AAON', 'AAPL']
   ETF (2826) ['AADR', 'AAXJ', 'ACWI', 'ACWX', 'ADRE', 'AGNG', 'AGZD', 'AIA', 'AIQ', 'AIRR']
   Funds (34926) ['AAAAX', 'AAACX', 'AAAEX', 'AAAFX', 'AAAGX', 'AAAHX', 'AAAIX', 'AAAJX', 'AAAKX', 'AAALX']
   ('alias',        ACT Symbol CQS Symbol
AAC=        AAC.U      AAC.U
AAC+        AAC.W     AAC.WS
AAIC-B     AAIC$B     AAICpB
AAIC-C     AAIC$C     AAICpC
AAM-A       AAM$A      AAMpA
...           ...        ...
XPOA=      XPOA.U     XPOA.U
XPOA+      XPOA.W    XPOA.WS
YCBD-A     YCBD$A     YCBDpA
ZEV+        ZEV.W     ZEV.WS
ZGN+        ZGN.W     ZGN.WS

[980 rows x 2 columns])

   '''
   test_file = find_files('./testConfig/testSectorStockList.ini')
   yahoo_list = []
   for sector, key,value in INI_READ.read(*test_file) :
       yahoo_list.extend(value)
   stock_list.extend(yahoo_list)
   unittest.main()