import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.figure as figure

def monthly_summary(stock_dict) :
    fig, ax = plt.subplots()
    fig.set_size_inches(10,10)
    ax.set_xlabel('Volatility')
    ax.set_ylabel('Performance vs SPY')
    ax.grid(color='lightgray', alpha=0.7)
    for stock in stock_dict.keys() :
        temp = stock_dict[stock]
        temp=pd.groupby(temp, by=[temp.index.month])
        ax.plot(temp.std(), temp.mean(),'or', ms=10, alpha=0.3)
        for label, x, y in zip(temp.groups.keys(), temp.std(), temp.mean()):
            plt.annotate(
                '{}-{}'.format(label,stock), 
                xy = (x, y), xytext = (20, -20),
                textcoords = 'offset points', ha = 'right', va = 'bottom',
                bbox = dict(boxstyle = 'round,pad=0.5', fc = 'yellow', alpha = 0.5),
                arrowprops = dict(arrowstyle = '->', connectionstyle = 'arc3,rad=0'))

def one_month_many_years(stock,dataDict) :
    fig, ax = plt.subplots()
    fig.set_size_inches(10,4)
    ax.set_xlabel('Month')
    ax.set_ylabel('{} vs SPY'.format(stock))
    ax.grid(color='lightgray', alpha=0.7)
    for x in dataDict.keys():
        dataSeries = dataDict[x]
        ax.plot(list(dataSeries.keys()), list(dataSeries.values()),
                '-o', alpha=0.5,
                label=x,
                linewidth=2,
                markersize=1)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc='right',bbox_to_anchor=(1.3, 0.5))
    
def one_month_year_to_year(data) :
    fig, ax = plt.subplots()
    fig.set_size_inches(10,4)
    ax.set_xlabel('Month')
    ax.set_ylabel('{} vs SPY'.format(stock))
    ax.grid(color='lightgray', alpha=0.7)
    for y in sorted(data.keys()) :
        data[y].plot()
        plt.title(y)
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc='right',bbox_to_anchor=(1.3, 0.5))
