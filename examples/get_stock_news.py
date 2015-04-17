yahoo_cash_flow = ScraperUtil.Yahoo(YahooFinance.get_cash_flow,WebUtils.format_as_soup,YahooParse.finance)
yahoo_income_statement = ScraperUtil.Yahoo(YahooFinance.get_income_statement,WebUtils.format_as_soup,YahooParse.finance)
yahoo_balance_sheet = ScraperUtil.Yahoo(YahooFinance.get_balance_sheet,WebUtils.format_as_soup,YahooParse.finance)
yahoo_analyst_estimates_soup = ScraperUtil.Yahoo(YahooFinance.get_analyst_estimates,WebUtils.format_as_soup,YahooParse.finance)

stocks = CacheService(NasDaqPandaDataService())
news = CacheService(YQL.NewsFeed())
temp = {}
for symbol in stocks().index.values :
    if symbol[0] not in temp.keys() :
        temp[symbol[0]] = []
    temp[symbol[0]].append(symbol)
for k in sorted(temp.keys()) :
    print ("{} - {}".format(k,len(temp[k])))

for symbol in stocks().index.values:
    print(symbol)
    print(news(symbol))
    break;
for symbol in stocks().index.values:
    print(symbol)
    for token in yahoo_cash_flow(symbol) :
        print (token)
    break
