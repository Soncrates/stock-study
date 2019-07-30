https://us.spindices.com/documents/spiva/spiva-us-year-end-2016.pdf

During the one-year period ending Dec.31, 2016, 66% of large-cap managers, 89.37% of mid-cap managers,and 85.54% of small-cap managers underperformed the S&P 500, the S&P MidCap 400, and the S&P SmallCap 600, respectively.These figures are on par with the one-year performance figures reported in June 2016,with the exception of large-cap managers,who faired relatively better.

Figures over the five-year period did not change significantly from the SPIVA U.S. Mid-Year 2016 Scorecard. During the five-year period ending Dec.31, 2016, 88.3% of large-cap managers, 89.95% of mid-cap managers, and 96.57% of small-cap managers underperformed their respective benchmarks.

Given that active managers’ performance can vary based on market cycles, the newly available 15-year data tells a more stable narrative.  Over the 15-year period ending Dec.2016, 92.15% of large-cap, 95.4% of mid-cap, and 93.21% of small-cap managers trailed their respective benchmarks.

Across all time horizons, the majority of managers across all international equity categories underperformed their benchmarks.

In other words, the odds you’ll do better than an index fund are close to 1 out of 20 when picking an actively-managed domestic equity mutual fund.

https://www.datacamp.com/community/tutorials/finance-python-trading

https://www.datacamp.com/community/tutorials/finance-python-trading#basics

df.describe()

# Inspect the first rows of November-December 2006
print(df.loc[pd.Timestamp('2006-11-01'):pd.Timestamp('2006-12-31')].head())

# Inspect the first rows of 2007 
print(df.loc['2007'].head())

# Inspect November 2006
print(df.iloc[22:43])

# Inspect the 'Open' and 'Close' values at 2006-11-01 and 2006-12-01
print(df.iloc[[22,43], [0, 3]])

# Sample 20 rows
sample = df.sample(20)

# Print `sample`
print(sample)

# Resample to monthly level 
monthly_df = df.resample('M').mean()

# Print `monthly_df`
print(monthly_df)

https://www.datacamp.com/community/tutorials/finance-python-trading#financialanalyses
# Import `numpy` as `np`
import numpy as np

# Assign `Adj Close` to `daily_close`
daily_close = df[['Adj Close']]

# Daily returns
daily_pct_change = daily_close.pct_change()

# Replace NA values with 0
daily_pct_change.fillna(0, inplace=True)

# Inspect daily returns
print(daily_pct_change)

# Daily log returns
daily_log_returns = np.log(daily_close.pct_change()+1)

# Print daily log returns
print(daily_log_returns)

# Resample `aapl` to business months, take last observation as value 
monthly = df.resample('BM').apply(lambda x: x[-1])

# Calculate the monthly percentage change
monthly.pct_change()

# Resample `aapl` to quarters, take the mean as value per quarter
quarter = df.resample("4M").mean()

# Calculate the quarterly percentage change
quarter.pct_change()


# Plot the distribution of `daily_pct_c`
daily_pct_change.hist(bins=50)

# Pull up summary statistics
print(daily_pct_change.describe())

# Calculate the cumulative daily returns
cum_daily_return = (1 + daily_pct_change).cumprod()

# Print `cum_daily_return`
print(cum_daily_return)

# Resample the cumulative daily return to cumulative monthly return 
cum_monthly_return = cum_daily_return.resample("M").mean()

# Print the `cum_monthly_return`
print(cum_monthly_return)
	
# Import the `api` model of `statsmodels` under alias `sm`
import statsmodels.api as sm

# Import the `datetools` module from `pandas`
from pandas.core import datetools

# Isolate the adjusted closing price
all_adj_close = all_data[['Adj Close']]

# Calculate the returns 
all_returns = np.log(all_adj_close / all_adj_close.shift(1))

# Isolate the AAPL returns 
aapl_returns = all_returns.iloc[all_returns.index.get_level_values('Ticker') == 'AAPL']
aapl_returns.index = aapl_returns.index.droplevel('Ticker')

# Isolate the MSFT returns
msft_returns = all_returns.iloc[all_returns.index.get_level_values('Ticker') == 'MSFT']
msft_returns.index = msft_returns.index.droplevel('Ticker')

# Build up a new DataFrame with AAPL and MSFT returns
return_data = pd.concat([aapl_returns, msft_returns], axis=1)[1:]
return_data.columns = ['AAPL', 'MSFT']

# Add a constant 
X = sm.add_constant(return_data['AAPL'])

# Construct the model
model = sm.OLS(return_data['MSFT'],X).fit()

# Print the summary
print(model.summary())

# Initialize the short and long windows
short_window = 40
long_window = 100

# Initialize the `signals` DataFrame with the `signal` column
signals = pd.DataFrame(index=aapl.index)
signals['signal'] = 0.0

# Create short simple moving average over the short window
signals['short_mavg'] = aapl['Close'].rolling(window=short_window, min_periods=1, center=False).mean()

# Create long simple moving average over the long window
signals['long_mavg'] = aapl['Close'].rolling(window=long_window, min_periods=1, center=False).mean()

# Create signals
signals['signal'][short_window:] = np.where(signals['short_mavg'][short_window:] 
                                            > signals['long_mavg'][short_window:], 1.0, 0.0)   

# Generate trading orders
signals['positions'] = signals['signal'].diff()

# Print `signals`
print(signals)

# Set the initial capital
initial_capital= float(100000.0)

# Create a DataFrame `positions`
positions = pd.DataFrame(index=signals.index).fillna(0.0)

# Buy a 100 shares
positions['AAPL'] = 100*signals['signal']   
  
# Initialize the portfolio with value owned   
portfolio = positions.multiply(aapl['Adj Close'], axis=0)

# Store the difference in shares owned 
pos_diff = positions.diff()

# Add `holdings` to portfolio
portfolio['holdings'] = (positions.multiply(aapl['Adj Close'], axis=0)).sum(axis=1)

# Add `cash` to portfolio
portfolio['cash'] = initial_capital - (pos_diff.multiply(aapl['Adj Close'], axis=0)).sum(axis=1).cumsum()   

# Add `total` to portfolio
portfolio['total'] = portfolio['cash'] + portfolio['holdings']

# Add `returns` to portfolio
portfolio['returns'] = portfolio['total'].pct_change()

# Print the first lines of `portfolio`
print(portfolio.head())

# Isolate the returns of your strategy
returns = portfolio['returns']

# annualized Sharpe ratio
sharpe_ratio = np.sqrt(252) * (returns.mean() / returns.std())

# Print the Sharpe ratio
print(sharpe_ratio)

https://www.datacamp.com/courses/intro-to-python-for-finance
https://www.datacamp.com/courses/importing-managing-financial-data-in-python


