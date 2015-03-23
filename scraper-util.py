def invoke_url(url,headers=None, raw=False) :
  import requests
  if headers is not None :
    ret = requests.get(url, headers=headers)        
  else :
    ret = requests.get(url)
  if not raw : ret = ret.text
  else : ret = ret.content
  return ret

def get_nasdaq_csv(exchange) : # from nasdaq.com
  headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
    'Accept-Encoding': 'none',
    'Accept-Language': 'en-US,en;q=0.8',
    'Connection': 'keep-alive',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
  }
  url = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=%s&render=download" % exchange
  return invoke_url(url,headers,raw=True)

def format_nasdaq(ret,unwanted_keys,exchange) :
  import pandas as pd
  ret['Exchange'] = pd.Series(exchange, index=ret.index)
  if unwanted_keys is not None  : 
    for key in unwanted_keys:
      if key in ret.columns.tolist() : ret.drop(key, 1)
  temp = ret.to_dict()
  if unwanted_keys is not None  : 
    for key in unwanted_keys:
      if key in temp.keys() : del temp[key]
  if len(temp.keys()) < len(ret.columns.tolist()) :
    ret = pd.DataFrame.from_dict(temp)
  return ret
def parse_csv(csv) :
    import pandas as pd
    import io
    ret = pd.read_csv(io.BytesIO(csv), encoding='utf8', sep=",",index_col="Symbol")
    return ret
    
def parse_number(s):
    ret=""
    try:
        ret = float(s)
    except ValueError:
        return s
    if ret - int(ret) == 0 : return int(ret)
    return ret

def get_morningstar_cash_flow(exchange_code, ticker) :
  url = 'http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?&t=%s:%s&region=usa&culture=en-US&cur=USD&reportType=cf&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw&r=963470&callback=jsonp%d&_=%d' % (exchange_code, ticker, int(time.time()), int(time.time()+150))
  return invoke_url(url)
def get_morningstar_income_statement(exchange_code, ticker) :
  url = 'http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?&t=%s:%s&region=usa&culture=en-US&cur=USD&reportType=is&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw&r=354589&callback=jsonp%d&_=%d' % (exchange_code, ticker, int(time.time()), int(time.time()+150))
  return invoke_url(url)
def get_morningstar_balance_sheet(exchange_code, ticker) :
  url = 'http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?&t=%s:%s&region=usa&culture=en-US&cur=USD&reportType=bs&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw'% (exchange_code, ticker)#&r=782238&callback=jsonp%d&_=%d' % (exchange_code, ticker, int(time.time()), int(time.time()+150))  
  return invoke_url(url)
def get_morningstar_key_ratios(exchange_code,ticker) :
  url = 'http://financials.morningstar.com/financials/getFinancePart.html?&callback=jsonp1408061143067&t=%s:%s&region=usa&culture=en-US&cur=USD&order=asc&_=1408061143210' % (exchange_code, ticker)
  return invoke_url(url)
  
def get_yahoo_cash_flow(ticker) :
  return invoke_url('http://finance.yahoo.com/q/cf?s=%s&annual' % ticker)
def get_yahoo_income_statement(ticker) :
  return invoke_url('http://finance.yahoo.com/q/is?s=%s&annual' % ticker)
def get_yahoo_balance_sheet(ticker) :
  return invoke_url('http://finance.yahoo.com/q/bs?s=%s&annual' % ticker)
def get_yahoo_analyst_estimates(ticker) :
  return invoke_url('http://finance.yahoo.com/q/ae?s=%s+Analyst+Estimates' % ticker)
def get_yahoo_rss_stock(ticker) :
  return invoke_url('http://finance.yahoo.com/rss/headline?s=%s' % ticker)
def get_yahoo_rss_industry(ticker) :
  return invoke_url('http://finance.yahoo.com/rss/industry?s=%s' % ticker)

def format_as_soup(url_response) :
  from bs4 import BeautifulSoup
  return BeautifulSoup(url_response)
def format_yahoo_finaance_rss(rss) :
  import xmltodict,re
  for item in re.findall(r'<item>(\w+)<\/item>', rss) :
    yield xmltodict.parse(item)
def parse_yahoo_1(soup) :
  factor = 1
  thousands = soup.body.findAll(text= "All numbers in thousands")
  if thousands : factor = 1000
  table = soup.find("table", { "class" : "yfnc_tabledata1" })
  prev = ''
  for cell in table.findAll(parse_yahoo3):
      text = cell.find(text=True)
      if not text : continue
      text = text.replace(u'\xa0', u' ')
      text = text.strip()
      if len(text) == 0: continue
      if text == prev : continue
      prev=text
      yield text,factor
def parse_yahoo_2(text) :
  text = " ".join(text.split()).replace(',', "")
  if len(text) == 0 : return ''
  if text[0] == "(":
    text_list = list(text)
    text_list[0] = "-"
    text_list[-1] = ""
    text = "".join(text_list)
  return parse_number(text)
def parse_yahoo3(tag) :
    if tag.name not in ['td','strong'] : return False
    return True
def parse_yahoo(soup) :
  for text,factor in parse_yahoo_1(soup) :
    text = parse_yahoo_2(text)
    if isinstance(text, str) :  yield text
    else : yield str(text*factor)
def get_yahoo_historical(stock,year,strict=True) :
    import pandas.io.data as pdd
    try :
        ret = pdd.DataReader(stock, data_source='yahoo', start='{}/1/1'.format(year))
    except IOError :
        return None
    if not strict : return ret
    first = ret.index.tolist()[0]
    if year != first.year : return None
    if 1 != first.month : return None
    return ret

class YQLQuery(object):
  _YAHOO_URL = 'http://query.yahooapis.com/v1/public/yql'
  _DATATABLES_URL = 'store://datatables.org/alltableswithkeys'
  _quotes = 'yahoo.finance.quotes'
  _options = 'yahoo.finance.options'
  _quoteslist = 'yahoo.finance.quoteslist'
  _sectors = 'yahoo.finance.sectors'
  _industry = 'yahoo.finance.industry'
  def __init__(self):
  def __call__(self, yql):
    import urllib
    import simplejson as json
    queryString = urllib.urlencode({'q': yql, 'format': 'json', 'env': self._DATATABLES_URL})
    url = self._YAHOO_URL  + '?' + queryString
    ret = invoke_url(url)
    ret = json.loads(ret)
    return ret
  def __is_valid_response(self, response, field):
    return 'query' in response and 'results' in response['query'] and field in response['query']['results']
  def __validate_response(self, response, tagToCheck):
    if self.__is_valid_response(response, tagToCheck):
      ret = response['query']['results'][tagToCheck]
    else:
      if 'error' in response:
        raise QueryError('YQL query failed with error: "%s".' % response['error']['description'])
      else:
        raise QueryError('YQL response malformed.')
    return ret
class YQLStock(object) :
  def __init__(self,query,table,tag) :
    self.query = query
    self.table = table
	  self.tag = tag
  def __call__(self,symbol, columns='*') :
    yql = 'select %s from %s where symbol in (%s)' %(columns, self.table, symbol)
    ret = self.query(yql)
    return self.query.__validate_response(ret, self.tag)
class YQLStockQuote(YQLStock) :
  def __init__(self) :
    YQLStock.__init__(YQLQuery(),YQLQuery._quotes,'quote')
class YQLStockOptions(YQLStock) :
  def __init__(self) :
    YQLStock.__init__(YQLQuery(),YQLQuery._options,'optionsChain')
class YQLStockQuoteSummary(YQLStock) :
  def __init__(self) :
    YQLStock.__init__(YQLQuery(),YQLQuery._quoteslist,'quote')
class YQLStockNameBySector(YQLStock) :
  def __init__(self) :
    YQLStock.__init__(YQLQuery(),YQLQuery._sectors,'sector')
  def __call__(self)
    yql = 'select * from %s' % self.table
    ret = self.query(yql)
    return self.query.__validate_response(ret, self.tag)
class YQLStockByIndustry(YQLStock) :
  def __init__(self) :
    YQLStock.__init__(YQLQuery(),YQLQuery._industry,'industry')
  def __call__(self,id)
    yql = 'select * from %s where id =\'%s\'' % self.table, id)
    ret = self.query(yql)
    return self.query.__validate_response(ret, self.tag)
class YQLNews(object) :
  RSS_URL = 'http://finance.yahoo.com/rss/headline?s='
  def __init__(self,query,table,tag) :
    self.query = query
  def __call__(self,symbol) :
    yql = 'select title, link, description, pubDate from rss where url=\'%s%s\'' % RSS_URL,symbol
    ret = self.query(yql)['query']['results']['item']
    if ret[0]['title'].find('not found') > 0:
      raise QueryError('Feed for %s does not exist.' % symbol)
    else:
      return ret
