import logging
import csv as _csv
import pandas as PD
from libCommon import FTP

'''
Web Scraping Utils

'''
class CONSTANTS() :
      FAMILY_FIELD = 'Fund Family Name' 
      PARTICIPANT_FIELD = 'MP Type'
      PARTICIPANT = {
              'A' : 'Agency Quote'
              , 'C' : 'Electronic Communications Network (ECN)'
              , 'E' : 'Exchange'
              , 'M' : 'Market Maker'
              , 'N' : 'Miscellaneous'
              , 'O' : 'Order Entry Firm'
              , 'P' : 'NASDAQ Participant'
              , 'Q' : 'Query Only Firm'
              , 'S' : 'Specialist'
              }
      MARKET_FIELD = 'Market Category'
      MARKET = { 'Q' : 'NASDAQ Global Select MarketSM'
                   , 'G' : 'NASDAQ Global MarketSM'
                   , 'S' : 'NASDAQ Capital Market'
                   }
      FINANCE_FIELD = 'Financial Status'
      FINANCE = { 'D' : 'Deficient: Issuer Failed to Meet NASDAQ Continued Listing Requirements'
                 , 'E' : 'Delinquent: Issuer Missed Regulatory Filing Deadline'
                 , 'Q' : 'Bankrupt: Issuer Has Filed for Bankruptcy'
                 , 'N' : 'Normal (Default): Issuer Is NOT Deficient, Delinquent, or Bankrupt.'
                 , 'G' : 'Deficient and Bankrupt'
                 , 'H' : 'Deficient and Delinquent'
                 , 'J' : 'Delinquent and Bankrupt'
                 , 'K' : 'Deficient, Delinquent, and Bankrupt'
                 }
      EXCHANGE_FIELD = 'Listing Exchange'
      EXCHANGE_FIELD2 = 'Exchange'
      EXCHANGE = {
              'A' : 'NYSE MKT'
              , 'N' : 'New York Stock Exchange (NYSE)'
              , 'P' : 'NYSE ARCA'
              , 'Z' : 'BATS Global Markets (BATS)'
              , 'V' : "Investors' Exchange, LLC (IEXG)"
              }
      TYPE_FIELD = 'Type'
      TYPE = { 'AN' : 'Annuities'
              , 'MF' : 'Mutual Fund'
              , 'MS' : 'Supplemental Mutual Fund'
              , '$$' : 'Money Market Fund'
              , '$S' : 'Supplemental Money Market Fund'
              , 'SP' : 'Structured Products'
              , 'US' : 'UIT Supplemental List'
              , 'UT' : 'UIT News Media List'
              }
      CATEGORY_FIELD = 'Category' 
      CATEGORY = {
              'M' : 'Variable'
              , 'N' : 'Equity Indexed'
              , 'O' : 'Open-end'
              , 'C' : 'Closed-end'
              , 'A' : 'General Purpose'
              , 'G' : 'Govt. Securities'
              , 'X' : 'Tax-Exempt'
              , 'R' : 'Access Transaction'
              , 'S' : 'Tax Driven Structure'
              , 'T' : 'Buffered Note'
              , 'U' : 'Principal Protected Note'
              , 'V' : 'Levered Note'
              , 'W' : 'Enhanced Income Note'
              , 'E' : 'Equity'
              , 'F' : 'Not Used/Reserved'
              , 'D' : 'Fixed Income'
              }
      NASDAQ_URL = 'ftp.nasdaqtrader.com'
      NASDAQ_FILE_LIST = ['bondslist.txt', 'bxoptions.txt', 'bxo_lmm.csv', 'bxtraded.txt'
              , 'gmniListedStrikesWithOptionIds.zip', 'iseListedStrikesWithOptionIds.zip'
              , 'mcryListedStrikesWithOptionIds.zip', 'mfundslist.txt', 'mpidlist.txt'
              , 'nasdaqlisted.txt', 'nasdaqtraded.txt', 'options.txt', 'otclist.txt', 'otherlisted.txt'
              , 'pbot.csv', 'phlxListedStrikesWithOptionIds.zip', 'phlxoptions.csv', 'phlxStrikesOld.zip', 'psxtraded.txt'
              , 'regnms', 'regsho', 'regshopilot', 'regshopilotlist', 'shorthalts'
              , 'TradingSystemAddsDeletes.txt']
class TRANSFORM_PARTICIPANT() :
      TYPE = CONSTANTS.PARTICIPANT.copy()
      TYPE_FIELD = CONSTANTS.PARTICIPANT_FIELD
      @classmethod
      def Type(cls,row) :
          value = row.get(cls.TYPE_FIELD,'') 
          row[cls.TYPE_FIELD] = cls.TYPE.get(value,value)
          return row
class TRANSFORM_STOCK() :
      MARKET = CONSTANTS.MARKET.copy()
      MARKET_FIELD = CONSTANTS.MARKET_FIELD
      FINANCE = CONSTANTS.FINANCE.copy()
      FINANCE_FIELD = CONSTANTS.FINANCE_FIELD
      EXCHANGE = CONSTANTS.EXCHANGE.copy()
      EXCHANGE_FIELD = CONSTANTS.EXCHANGE_FIELD
      EXCHANGE_FIELD2 = CONSTANTS.EXCHANGE_FIELD2
      @classmethod
      def Market(cls,row) :
          value = row.get(cls.MARKET_FIELD,'') 
          row[cls.MARKET_FIELD] = cls.MARKET.get(value,value)
          return row
      @classmethod
      def Finance(cls,row) :
          value = row.get(cls.FINANCE_FIELD,'') 
          row[cls.FINANCE_FIELD] = cls.FINANCE.get(value,value)
          return row
      @classmethod
      def Exchange(cls,row) :
          value = row.get(cls.EXCHANGE_FIELD,'') 
          row[cls.EXCHANGE_FIELD] = cls.EXCHANGE.get(value,value)
          return row
      @classmethod
      def Exchange2(cls,row) :
          value = row.get(cls.EXCHANGE_FIELD2,'') 
          row[cls.EXCHANGE_FIELD2] = cls.EXCHANGE.get(value,value)
          return row
class TRANSFORM_FUND() :
      TYPE = CONSTANTS.TYPE.copy()
      TYPE_FIELD = CONSTANTS.TYPE_FIELD
      CATEGORY = CONSTANTS.CATEGORY.copy()
      CATEGORY_FIELD = CONSTANTS.CATEGORY_FIELD
      @classmethod
      def Type(cls,row) : 
          value = row.get(cls.TYPE_FIELD,'') 
          row[cls.TYPE_FIELD] = cls.TYPE.get(value,value)
          return row
      @classmethod
      def Category(cls,row) : 
          value = row.get(cls.CATEGORY_FIELD,'') 
          row[cls.CATEGORY_FIELD] = cls.CATEGORY.get(value,value)
          return row

class NASDAQ() :
      URL = CONSTANTS.NASDAQ_URL
      file_list = CONSTANTS.NASDAQ_FILE_LIST.copy()
      FAMILY_FIELD = CONSTANTS.FAMILY_FIELD

      def __init__(self,ftp,*file_list) :
          self.ftp = ftp
          self.file_list = file_list
      def listed(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[9])
          ret = NASDAQ.to_list(raw)
          ret = map(lambda row : TRANSFORM_STOCK.Market(row), ret)
          ret = map(lambda row : TRANSFORM_STOCK.Finance(row), ret)
          ret = NASDAQ.to_dict('Symbol',*ret)
          ret = NASDAQ.to_pandas(**ret)

          keys = ret.filter(regex="Test", axis=1).values.ravel()
          keys = PD.unique(keys)
          logging.info(keys)

          test = ret[ret['Test Issue']=='Y']
          ret = ret[ret['Test Issue'] == 'N']
          logging.debug(test)

          return ret, raw 
      def traded(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[10])
          ret = NASDAQ.to_list(raw)
          ret = map(lambda row : TRANSFORM_STOCK.Market(row), ret)
          ret = map(lambda row : TRANSFORM_STOCK.Finance(row), ret)
          ret = map(lambda row : TRANSFORM_STOCK.Exchange(row), ret)
          ret = NASDAQ.to_dict('Symbol',*ret)
          ret = NASDAQ.to_pandas(**ret)

          keys = ret.filter(regex="Test", axis=1).values.ravel()
          keys = PD.unique(keys)
          logging.info(keys)

          test = ret[ret['Test Issue']=='Y']
          ret = ret[ret['Test Issue'] == 'N']
          logging.debug(test)
          return ret, raw 
      def other(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[13])
          ret = NASDAQ.to_list(raw)
          logging.debug(len(ret))
          ret = map(lambda row : TRANSFORM_STOCK.Exchange2(row), ret)
          ret = NASDAQ.to_dict('NASDAQ Symbol',*ret)
          logging.debug(len(ret))
          ret = NASDAQ.to_pandas(**ret)

          test = ret[ret['Test Issue']=='Y']
          ret = ret[ret['Test Issue'] == 'N']
          logging.debug(test)
          return ret, raw 
      def funds(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[7])
          ret = NASDAQ.to_list(raw)
          ret = map(lambda row : TRANSFORM_FUND.Type(row), ret)
          ret = map(lambda row : TRANSFORM_FUND.Category(row), ret)
          ret = NASDAQ.to_dict('Fund Symbol',*ret)
          ret = NASDAQ.to_pandas(**ret)
          return ret, raw 
      def bonds(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[0])
          ret = NASDAQ.to_list(raw)
          ret = NASDAQ.to_dict('Symbol',*ret)
          ret = NASDAQ.to_pandas(**ret)
          return ret, raw
      def participants(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[8])
          ret = NASDAQ.to_list(raw)
          ret = map(lambda row : TRANSFORM_PARTICIPANT.Type(row), ret)
          ret = NASDAQ.to_dict('MPID',*ret)
          ret = NASDAQ.to_pandas(**ret)
          return ret, raw 
      def stock_list(self) :
          listed, csv = self.listed()
          #traded, csv = self.traded()
          other, csv = self.other()
          total = listed.rename(columns={'Market Category' : 'Exchange'})
          total = total.filter(items=['Exchange', 'ETF','Security Name'])
          x = other.filter(items=['Exchange', 'ETF', 'Security Name'])
          total = PD.concat([total,x])

          etf = total[total['ETF']=='Y']
          stock = total[total['ETF']!='Y']
          keys = etf.filter(regex="Exchange", axis=1).values.ravel()
          keys = PD.unique(keys)
          logging.info(keys)
          keys = stock.filter(regex="Exchange", axis=1).values.ravel()
          keys = PD.unique(keys)
          logging.info(keys)

          alias = other.filter(regex="Symbol", axis=1)
          for column in alias.columns.values.tolist() :
              for key in alias.index.values.tolist() :
                  alias = alias[alias[column]!=key]
          logging.info(len(alias))

          logging.info(('Stocks',len(stock)))
          logging.info(('ETF',len(etf)))
          logging.info(('alias',len(alias)))
          return stock, etf, alias 
      def fund_list(self) :
          ret, csv = self.funds()
          logging.info(('Funds',len(ret)))
          return ret
      def by_family(self) :
          ret = {}
          fund_list, csv = self.funds()
          for i, fund in enumerate(fund_list) :
              name = fund.pop(self.FAMILY_FIELD,self.FAMILY_FIELD)
              if name not in ret :
                 ret[name] = []
              ret[name].append(fund)
          for i, family in enumerate(ret) :
              logging.debug((family,ret[family][0]))
          logging.info(('Funds',len(fund_list)))
          return fund_list

      @classmethod
      def init(cls) :
          ftp = FTP.init(server=cls.URL)
          file_list = map(lambda x : '/symboldirectory/{}'.format(x), cls.file_list)
          ret = cls(ftp,*file_list)
          return ret
      @classmethod
      def to_list(cls,ret) :
          data = str(ret)
          reader = _csv.DictReader(data.split('\n'), delimiter='|')
          ret = []
          for i, row in enumerate(reader):
              ret.append(row)
          return ret
      @classmethod
      def to_dict(cls, target, *entry_list) :
          logging.debug(entry_list[0])
          ret = {}
          for entry in entry_list :
              key = entry.pop(target,target)
              if not isinstance(key,str) :
                 logging.warning(key)
                 logging.warning(type(key))
                 continue
              ret[key] = dict(zip(entry,entry.values()))
              if len(ret) == 1 :
                 logging.debug(ret)
          return ret
      @classmethod
      def to_pandas(cls, **kwargs) :
          _columns = list(kwargs[list(kwargs.keys())[0]].keys())
          logging.debug(_columns)
          ret = PD.DataFrame.from_dict(kwargs, orient='index',columns=_columns)
          return ret

if __name__ == '__main__' :
   import logging
   import sys
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   obj = NASDAQ.init()
   #listed, csv = obj.listed()
   #funds, csv = obj.funds()
   #bonds, csv = obj.bonds()
   #traded, csv = obj.traded()
   #other, csv = obj.other()
   #participants, csv = obj.participants()
   #print obj()

   stock, etf, alias = obj.stock_list()
   print (stock[:10])
   #obj.fund_list()
