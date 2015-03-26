import pandas as pd
def one_month_many_years(key,dataSeries):
    lastday = getLastDay(key[0],key[1])
    days = range(1,lastday+1)
    ret=dict.fromkeys(days,None)
    for x in sorted(dataSeries.index) :
        ret[x.day]=dataSeries[x]
    return ret
def gen_normalize_by_month_1(stock,pct,month):
    pct=pct.ix[pct.index.month==month]
    dataGroup = pd.groupby(pct,by=[pct.index.month,pct.index.year])
    for key in dataGroup.groups :
        _month = dataGroup.get_group(key)
        _month = (_month + 1).cumprod()
        _month['normalized']=(_month[stock]-_month['spx']) + 1
        yield key, _month, _month['normalized']
def gen_normalize_by_month(stock,pct, month) :
    for key,_month, normalized in gen_normalize_by_month_1(stock,pct, month) :
        label = '{},{}-{}'.format(key[1],key[0],stock)
        temp=one_month_many_years(key,normalized)
        yield key[1], _month, label,temp 

class CompareStock(object) :
  def __init__(self,service = StockService()) :
     self.spy = 'spx'
     self.spy_symbol = '^GSPC'
     self.service = service
  def __service(self,contender) :
    spy = self.service(self.spy_symbol)
    ret = self.service(contender)
    spy2 = spy.reindex(ret.index)
    return ret, spy2
  def __normalize(self,both,spy,contender) :
    ret_1 = both.pct_change()
    ret_2= ret_1[contender]-ret_1[spy]
    return contender,ret_1,ret_2
  def byAdjClose(self,contender) :
    temp, spy = self.__service(contender)
    both = pd.DataFrame(data = {self.spy: spy['Adj Close'], contender : temp['Adj Close']})
    return self.__normalize(both,self.spy,contender)
  def byVolume(self,contender) :
    temp, spy = self.__service(contender)
    both = pd.DataFrame(data = {self.spy: spy['Volume'], contender : temp['Volume']})
    return self.__normalize(both,self.spy,contender)
