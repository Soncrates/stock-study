yahoo_cash_flow = ScraperUtil.Yahoo(YahooFinance.get_cash_flow,WebUtils.format_as_soup,YahooParse.finance)
yahoo_income_statement = ScraperUtil.Yahoo(YahooFinance.get_income_statement,WebUtils.format_as_soup,YahooParse.finance)
yahoo_balance_sheet = ScraperUtil.Yahoo(YahooFinance.get_balance_sheet,WebUtils.format_as_soup,YahooParse.finance)
yahoo_analyst_estimates_soup = ScraperUtil.Yahoo(YahooFinance.get_analyst_estimates,WebUtils.format_as_soup,YahooParse.finance)

yahoo_cash_flow = CacheService(yahoo_cash_flow)
yahoo_income_statement = CacheService(yahoo_income_statement)
yahoo_balance_sheet = CacheService(yahoo_balance_sheet)
yahoo_analyst_estimates_soup = CacheService(yahoo_analyst_estimates_soup)

stocks = CacheService(NasDaqPandaDataService())
news = CacheService(YQL.NewsFeed())
symbol_list_list = CollectionUtils.chunkify(sorted(stocks().index.values))

for symbol_list in symbol_list_list :
    print(symbol_list)
    print(news(symbol_list))
    break

for symbol in stocks().index.values:
    print(symbol)
    print(news(symbol))
    break
for symbol in stocks().index.values:
    print(symbol)
    for token in yahoo_cash_flow(symbol) :
        print (token)
    break
