from transforms import compare-to-Snp as Snp
from plotting import compare-to-Snp as plot

stock_list=['IEX','UPS','D','SHW','GSK','PH']
compare = Snp.CompareStock()
pct_dict = {}
diff_dict = {}
for symbol, pct,diff in compare(stock_list):
    pct_dict[symbol] = pct
    diff_dict[symbol] = diff
plot.monthly_summary(diff_dict)

month_list=[11]
for stock in pct_dict.keys() :
    temp_1 = {}
    temp_2 = {}
    for k1,v1,k2,v2 in Snp.gen_normalize_by_month(stock,pct_dict[stock],month_list) :
        temp_1[k1] = v1
        temp_2[k2] = v2
    plot.one_month_many_years(stock,temp_2)
    plot.one_month_year_to_year(temp_1)
