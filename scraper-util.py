def get_stock_from_nasdaq(exchanges=["nyse", "nasdaq"]) : # from nasdaq.com
	headers = {
		'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8',
		'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,;q=0.3',
		'Accept-Encoding': 'none',
		'Accept-Language': 'en-US,en;q=0.8',
		'Connection': 'keep-alive',
		'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,/*;q=0.8'
	}
	for exchange in exchanges :
		yield "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=%s&render=download" % exchange, headers

def get_morningstar_cash_flow(exchange_code, ticker) :
  return 'http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?&t=%s:%s&region=usa&culture=en-US&cur=USD&reportType=cf&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw&r=963470&callback=jsonp%d&_=%d' % (exchange_code, ticker, int(time.time()), int(time.time()+150))
def get_morningstar_income_statement(exchange_code, ticker) :
  return  'http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?&t=%s:%s&region=usa&culture=en-US&cur=USD&reportType=is&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw&r=354589&callback=jsonp%d&_=%d' % (exchange_code, ticker, int(time.time()), int(time.time()+150))
def get_morningstar_balance_sheet(exchange_code, ticker) :
  return 'http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?&t=%s:%s&region=usa&culture=en-US&cur=USD&reportType=bs&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw'% (exchange_code, ticker)#&r=782238&callback=jsonp%d&_=%d' % (exchange_code, ticker, int(time.time()), int(time.time()+150))
def get_morningstar_key_ratios(exchange_code,ticker) :
  return 'http://financials.morningstar.com/financials/getFinancePart.html?&callback=jsonp1408061143067&t=%s:%s&region=usa&culture=en-US&cur=USD&order=asc&_=1408061143210' % (exchange_code, ticker)

def get_yahoo_cash_flow(ticker) :
  return 'http://finance.yahoo.com/q/cf?s=%s&annual' % ticker
def get_yahoo_cash_flow_soup(ticker) :
  ticker = get_yahoo_cash_flow(ticker)
  return BeautifulSoup(invoke_url(ticker), convertEntities=BeautifulSoup.HTML_ENTITIES)
def get_yahoo_income_statement(ticker) :
  return 'http://finance.yahoo.com/q/is?s=%s&annual' % ticker
def get_yahoo_income_statement_soup(ticker) :
  ticker = get_yahoo_income_statement(ticker)
  return BeautifulSoup(invoke_url(ticker), convertEntities=BeautifulSoup.HTML_ENTITIES)
def get_yahoo_balance_sheet(ticker) :
  return 'http://finance.yahoo.com/q/bs?s=%s&annual' % ticker
def get_yahoo_balance_sheet_soup(ticker) :
  ticker = get_yahoo_balance_sheet(ticker)
  return BeautifulSoup(invoke_url(ticker), convertEntities=BeautifulSoup.HTML_ENTITIES)
def get_yahoo_analyst_estimates(ticker) :
  return 'http://finance.yahoo.com/q/ae?s=%s+Analyst+Estimates' % ticker
def get_yahoo_analyst_estimates_soup(ticker) :
  ticker = get_yahoo_analyst_estimates(ticker)
  return BeautifulSoup(invoke_url(ticker), convertEntities=BeautifulSoup.HTML_ENTITIES)


def invoke_url(url,headers) :
	import urllib.request
	response = urllib.request.Request(url, headers=headers)
	ret = urllib.request.urlopen(response)
	ret = ret.read()
	if isinstance(ret, str): # Python2
		return ret
	elif isinstance(ret, bytes): # Python3
		return ret.decode("utf-8")
def parse_yahoo_1(soup) :
	factor = 1
	thousands = soup.body.findAll(text= "All numbers in thousands")
	if thousands : factor = 1000
	table = soup.find("table", { "class" : "yfnc_tabledata1" })
	for tag in ['td','strong'] :
	  for cell in table.findAll(tag):
	    text = cell.find(text=True)
	    if text: yield text.replace(u'\xa0', u' '),factor
def parse_yahoo_2(text) :
  text = " ".join(text.split()).replace(',', "")
  if text[0] == "(":
  	text_list = list(text)
	text_list[0] = "-"
	text_list[-1] = ""
	text = "".join(text_list)
  return parse_number(text)
def parse_yahoo(soup) :
  for text,factor in parse_yahoo_1(soup) :
    text = parse_yahoo_2(text)
    if isinstance(text, str) :  yield text
    else : yield str(text*factor))
def parse_csv_stock_symbol_symbols(csv):
  lines = csv.splitlines()
  rr = range(0,len(lines))
  keys = []
  for r in rr :
    ret = lines[r].replace('","',"|").split('|')
    if len(ret) == 0 : continue
    ret = list(map(lambda x : x.replace('"',''), ret))
    ret = list(map(lambda x : x.replace(',',''), ret))
    if r == 0 :
      keys=ret
      continue
    if len(keys) == len(ret) : ret = dict(zip(keys,ret))
    yield ret
def parse_number(s):
    ret=""
    try:
        ret = float(s)
    except ValueError:
        return s
    if ret - int(ret) == 0 : return int(ret)
    return ret
