import logging as log
import csv as CVS
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

def filter_test(ret) :
    keys = ret.filter(regex="Test", axis=1).values.ravel()
    keys = PD.unique(keys)
    log.info(('test',keys))
    test = ret[ret['Test Issue']=='Y']
    ret = ret[ret['Test Issue'] == 'N']
    log.debug(test)
    return ret

def is_unexpected(key) :
    if not key :
       return True
    if not isinstance(key,str) :
       log.warning(key)
       log.warning(type(key))
       return True
    return False
    
def transform_stocks(listed,other) :
    ret = listed.rename(columns={'Market Category' : 'Exchange'})
    ret = ret.filter(items=['Exchange', 'ETF','Security Name'])
    other_list = other.filter(items=['Exchange', 'ETF', 'Security Name'])
    ret = PD.concat([ret,other_list])
    return ret
def transform_listed(row_list) :
    row_list = transform_to_list(row_list)
    ret = [ TRANSFORM_STOCK.Market(row) for row in row_list]
    ret = [ TRANSFORM_STOCK.Finance(row) for row in ret]
    ret = transform_to_dict('Symbol',*ret)
    ret = transform_to_pandas(**ret)
    log.debug(ret)
    return ret
def transform_traded(row_list) :
    row_list = transform_to_list(row_list)
    ret = [ TRANSFORM_STOCK.Market(row) for row in row_list]
    ret = [ TRANSFORM_STOCK.Finance(row) for row in ret]
    ret = [ TRANSFORM_STOCK.Exchange(row) for row in ret]
    ret = transform_to_dict('Symbol',*ret)
    ret = transform_to_pandas(**ret)
    log.debug(ret)
    return ret
def transform_other(row_list) :
    row_list = transform_to_list(row_list)
    log.debug(('other',len(row_list)))
    ret = [ TRANSFORM_STOCK.Exchange2(row) for row in row_list]
    ret = transform_to_dict('NASDAQ Symbol',*ret)
    log.debug(('other',len(ret)))
    ret = transform_to_pandas(**ret)
    log.debug(ret)
    return ret
def transform_fund(row_list) :
    row_list = transform_to_list(row_list)
    ret = [ TRANSFORM_FUND.Type(row) for row in row_list ]
    ret = [ TRANSFORM_FUND.Category(row)for row in ret ]
    ret = transform_to_dict('Fund Symbol',*ret)
    ret = transform_to_pandas(**ret)
    log.debug(ret)
    return ret
def transform_bond(row_list) :
    ret = transform_to_list(row_list)
    ret = transform_to_dict('Symbol',*ret)
    ret = transform_to_pandas(**ret)
    log.debug(ret)
    return ret
def transform_participants(row_list) :
    row_list = transform_to_list(row_list)
    ret = [ TRANSFORM_PARTICIPANT.Type(row) for row in row_list]
    ret = transform_to_dict('MPID',*ret)
    ret = transform_to_pandas(**ret)
    log.debug(ret)
    return ret

def transform_to_list(ret) :
    data = str(ret)
    reader = CVS.DictReader(data.split('\n'), delimiter='|')
    ret = []
    for i, row in enumerate(reader):
        ret.append(row)
    return ret
def transform_to_dict(target, *entry_list) :
    log.debug(entry_list[0])
    ret = {}
    for entry in entry_list :
        key = entry.pop(target,target)
        if is_unexpected(key) :
            continue
        ret[key] = dict(zip(entry,entry.values()))
    #if len(ret) > 1 :
    #   log.debug(ret)
    return ret
def transform_to_pandas(**kwargs) :
    column_list = list(kwargs[list(kwargs.keys())[0]].keys())
    log.debug(column_list)
    ret = PD.DataFrame.from_dict(kwargs, orient='index',columns=column_list)
    return ret    

class NASDAQ() :
      URL = CONSTANTS.NASDAQ_URL
      file_list = CONSTANTS.NASDAQ_FILE_LIST.copy()
      FAMILY_FIELD = CONSTANTS.FAMILY_FIELD

      def __init__(self,ftp,*file_list) :
          self.ftp = ftp
          self.file_list = file_list
      def extract_listed(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[9])
          ret = transform_listed(raw)
          ret = filter_test(ret)
          return ret, raw 
      def extract_traded_list(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[10])
          ret = transform_traded(raw)
          ret = filter_test(ret)
          return ret, raw 
      def extract_other_list(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[13])
          ret = transform_other(raw)
          ret = filter_test(ret)
          return ret, raw 
      def extract_fund_list(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[7])
          ret = transform_fund(raw)
          return ret, raw 
      def extract_bond_list(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[0])
          ret = transform_bond(raw)
          return ret, raw
      def extract_participant_list(self) :
          raw = FTP.GET(self.ftp, pwd = self.file_list[8])
          ret = transform_participants(raw)
          return ret, raw 
      def extract_stock_list(self) :
          listed, csv = self.extract_listed()
          #traded, csv = self.traded()
          other, csv = self.extract_other_list()
          total = transform_stocks(listed, other)

          etf_list  = total[total['ETF']=='Y']
          stock_list = total[total['ETF']!='Y']
          
          keys = etf_list.filter(regex="Exchange", axis=1).values.ravel()
          keys = PD.unique(keys)
          log.info(('etf',len(keys),keys))
          keys = stock_list.filter(regex="Exchange", axis=1).values.ravel()
          keys = PD.unique(keys)
          log.info(('stock',len(keys),keys))

          log.info(('Stocks',len(stock_list)))
          log.info(('ETF',len(etf_list)))
          return stock_list, etf_list, other
      def by_family(self) :
          ret = {}
          fund_list, csv = self.funds()
          for i, fund in enumerate(fund_list) :
              name = fund.pop(self.FAMILY_FIELD,self.FAMILY_FIELD)
              if name not in ret :
                 ret[name] = []
              ret[name].append(fund)
          for i, family in enumerate(ret) :
              log.debug((family,ret[family][0]))
          log.info(('Funds',len(fund_list)))
          return fund_list

      @classmethod
      def init(cls) :
          ftp = FTP.init(server=cls.URL)
          file_list = ['/symboldirectory/{}'.format(file) for file in cls.file_list ]
          ret = cls(ftp,*file_list)
          return ret