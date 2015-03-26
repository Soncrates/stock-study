import scrapers

nasdaqScraper = NasdaqScraper(get_nasdaq_csv,parse_csv,format_nasdaq)
nasdaqScraper_alt = NasdaqScraper(get_nasdaq_csv,parse_csv,data_formatter=None)
yahoo_cash_flow = YahooScraper(get_yahoo_cash_flow,format_as_soup,parse_yahoo)
yahoo_income_statement = YahooScraper(get_yahoo_income_statement,format_as_soup,parse_yahoo)
yahoo_balance_sheet = YahooScraper(get_yahoo_balance_sheet,format_as_soup,parse_yahoo)
yahoo_analyst_estimates_soup = YahooScraper(get_yahoo_analyst_estimates,format_as_soup,parse_yahoo)

stocks = NasdaqService(nasdaqScraper)

news = YQLNewsList()
for symbol in stocks().index.values:
    print(symbol)
    print(news(symbol))
#    for token in yahoo_cash_flow(symbol) :
#        print (token)
