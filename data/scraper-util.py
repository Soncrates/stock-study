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

def format_yahoo_finance_rss(rss) :
  import xmltodict,re
  for item in re.findall(r'<item>(\w+)<\/item>', rss) :
    yield xmltodict.parse(item)
