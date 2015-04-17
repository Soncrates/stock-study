class MorningStar(object) : 
    @staticmethod
    def get_cash_flow(exchange_code, ticker) :
      url = 'http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?&t=%s:%s&region=usa&culture=en-US&cur=USD&reportType=cf&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw&r=963470&callback=jsonp%d&_=%d' % (exchange_code, ticker, int(time.time()), int(time.time()+150))
      return WebUtils.invoke_url(url)
    @staticmethod
    def get_income_statement(exchange_code, ticker) :
      url = 'http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?&t=%s:%s&region=usa&culture=en-US&cur=USD&reportType=is&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw&r=354589&callback=jsonp%d&_=%d' % (exchange_code, ticker, int(time.time()), int(time.time()+150))
      return WebUtils.invoke_url(url)
    @staticmethod
    def get_balance_sheet(exchange_code, ticker) :
      url = 'http://financials.morningstar.com/ajax/ReportProcess4HtmlAjax.html?&t=%s:%s&region=usa&culture=en-US&cur=USD&reportType=bs&period=12&dataType=A&order=asc&columnYear=5&rounding=3&view=raw'% (exchange_code, ticker)#&r=782238&callback=jsonp%d&_=%d' % (exchange_code, ticker, int(time.time()), int(time.time()+150))  
      return WebUtils.invoke_url(url)
    @staticmethod
    def get_key_ratios(exchange_code,ticker) :
      url = 'http://financials.morningstar.com/financials/getFinancePart.html?&callback=jsonp1408061143067&t=%s:%s&region=usa&culture=en-US&cur=USD&order=asc&_=1408061143210' % (exchange_code, ticker)
      return WebUtils.invoke_url(url)
class YahooFinance(object) : 
    @staticmethod
    def get_cash_flow(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/q/cf?s=%s&annual' % ticker)
    @staticmethod
    def get_income_statement(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/q/is?s=%s&annual' % ticker)
    @staticmethod
    def get_balance_sheet(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/q/bs?s=%s&annual' % ticker)
    @staticmethod
    def get_analyst_estimates(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/q/ae?s=%s+Analyst+Estimates' % ticker)
    @staticmethod
    def get_rss_stock(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/rss/headline?s=%s' % ticker)
    @staticmethod
    def get_rss_industry(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/rss/industry?s=%s' % ticker)

    @staticmethod
    def format_yahoo_finance_rss(rss) :
      import xmltodict,re
      for item in re.findall(r'<item>(\w+)<\/item>', rss) :
        yield xmltodict.parse(item)
  
class StockUtils(object) :
    @staticmethod
    def get_exchange_code(stock) :
      if stock is None : return None
      exchange = getattr(stock, "StockExchange_yf")
      if exchange == 'NYSE':
        exchange_code = "XNYS"
      elif exchange == "NasdaqNM":
        exchange_code = "XNAS"
