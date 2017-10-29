#backtest logic 
#gethistrocalData, 
#holding_status = NULL 
#price

#parameter 

# every 5 min

    #iteration: get 25 hours data, prepare historical data, hodling status for tradingLogic, set_holding_status (weifeng's logic), 
    #weiFeng??price and holding_status update frequency
    #output marketname, timestamp, price, buy_signal, sell_signal, holding_status

#test



#next, 1. import; 2. read WEifeng code; 

import urllib2 #only available for python 2.7, not for python 3.6
	#from urllib.request import urlopen
import json
import matplotlib.pyplot as plt
import matplotlib
from pandas import pandas as pd
import numpy as np
import os

#q1: what is class? 

#get data 
#market="BTC-ETH"
def readData(market="BTC-MONA"):
	timestamp='1499127220008'
	#market="USDT-BTC"
	#this command has error in python3.6, it is why we use python 2.7
	data=json.loads(urllib2.urlopen("https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName="+market+"&tickInterval=fiveMin&_="+timestamp).read())['result']
	data2=json.loads(urllib2.urlopen("https://bittrex.com/api/v1.1/public/getmarketsummaries").read())['result']
	df = pd.DataFrame(data).set_index("T")
	df.index = pd.DatetimeIndex(df.index)
	#define returns 
	df['returns'] = np.log(df['C'] / df['C'].shift(1))
	#24hour V 
	df['24hour_V']=df['BV'].rolling(288,min_periods=288).sum() 
	df['24hour_V_gt']=(df['24hour_V']>=300)
	df.describe()
	df['V_BTC']=df['V']*df['C']
	df['V_BTC_5min']=df['V_BTC'].rolling(1).sum()
	return(df)





    
df=readData(market='BTC-GRS')


#buy #define rolling volume, diff , if diff> threshold, buy
def buySignal(df,num_windows,refresh_frequency=1, Vthreshold=0.1, Pthreshold=0.01):
	i=num_windows
	print i 
	col='rollingV_%s' %i
	col_2='rollingV_diff_%s' %i 
	col_3='rollingV_ind_%s' %i 
	df[col]=df['V'].rolling(i,min_periods=i).mean()
	#v_cols=v_cols.append(col) #append has error , debug here 
	df[col_2]=(df[col]-df[col].shift(refresh_frequency))/df[col].shift(refresh_frequency)
	df[col_3]=np.where(df[col_2]>Vthreshold, 1, 0)
	col_4='price_ind_%s' %i
	df[col_4]=np.where((df['C']-df['C'].shift(num_windows))/df['C'].shift(num_windows)>Pthreshold,1,0)
	df['ind_both']=df[col_3]* df[col_4]
	df['buy_ind']=np.where((df['ind_both']-df['ind_both'].shift(1)==1),1,0)
	return(df) 
#next, test this code here, debug	
df=buySignal(df=df,num_windows=12,refresh_frequency=1, Vthreshold=0.5, Pthreshold=0.05)

def saveDate(market):
	df=readData(market)
	df=buySignal(df=df,num_windows=60,refresh_frequency=5, Vthreshold=0.5, Pthreshold=0.05)
	print df.columns.values
	print df[70:80]
	colNames=['V', 'V_BTC','V_BTC_5min', 'rollingV_60', 'rollingV_diff_60', "C"]
	df2=df[colNames]
	df2.to_csv("/Users/mengyeguo/Documents/Bittrex/"+str(market)+".csv")



#sell: find the buy signal, loop after the buy signal to get sell position, get signal 

def sellSignal(df,thre_drop=0.01,recent_days = 7): # 
	price_buy=np.nan
	price_peak=np.nan
	df['sell_ind']=0
	df['buy_new']=0
	df['holding_status']=0
	df['price_buy']=np.nan
	df['price_peak']=np.nan

	df_tmp=df[int((len(df)-recent_days*24*60/5)):len(df)]#len(df)]
	df=df_tmp
	#start_time = time.time()
	for i,data_i in df.iterrows():
		row_i=df.index.get_loc(i)
		print row_i
		if(row_i==0):
			df.loc[i,'sell_ind']=0
		elif(row_i>0):
			index_previous=df.index[row_i-1]
			holding_status= (df.loc[:index_previous,'buy_new'] - df.loc[:index_previous,'sell_ind']).sum()
			print holding_status
			print data_i['buy_new']
			print data_i['sell_ind']
			df.loc[i,'holding_status']=holding_status
			if holding_status==0 and data_i['buy_ind']==1:
				df.loc[i,'buy_new']=1 
				print data_i
				price_buy=data_i['C']
				price_peak = data_i['C']
			elif holding_status==1:
				if holding_status == 1 and (data_i['C']-price_buy)/price_buy<-thre_drop and (data_i['C']-price_peak)/price_peak<-0.1: 
					print data_i
					df.loc[i,'sell_ind']=1
					price_buy=np.nan
					price_peak=np.nan
				elif data_i['C']> price_peak:
					price_peak = data_i['C']
			df.loc[i,'price_buy']=price_buy
			df.loc[i,'price_peak']=price_peak
	return(df)

df=sellSignal(df=df, thre_drop=0.05,recent_days=0.1)#n_max=len(df) should be defaults

#elapsed_time = time.time() - start_time




def plotData(df=df,market='BTC-OMG',num_windows=12, refresh_frequency = 1, Vthreshold=0.5, Pthreshold=0.05,  outputLocation='/Users/mengyeguo/Documents/Bittrex/'):
	df['mystrategy_shi'] = df['holding_status'].shift(1) *df['returns']
	col_name=['returns', 'mystrategy_shi']
	df[col_name].dropna().cumsum().apply(np.exp).plot()
	titleName= 'lookBackWindow='+str(num_windows)+',refresh='+str(refresh_frequency)+',Vthre='+str(Vthreshold)+',Pthre='+str(Pthreshold)
	plt.title(titleName)
	#plt.show()
	fileName=str(market)+'.png'
	plt.savefig(outputLocation+fileName, bbox_inches='tight')
	
	return_vec=df[col_name].dropna().cumsum().apply(np.exp)
	return(return_vec)

plotData(df, market='BTC-GRS')



def oneTrading(market,num_windows,refresh_frequency, Vthreshold, Pthreshold, thre_drop, recent_days):
	df=readData(market=market)
	df=buySignal(df=df,num_windows=num_windows,refresh_frequency=refresh_frequency, Vthreshold=Vthreshold, Pthreshold=Pthreshold)
	df=sellSignal(df=df, thre_drop=thre_drop,recent_days = recent_days)
	plotData(df=df, market=market, num_windows=num_windows,refresh_frequency=refresh_frequency, Vthreshold=Vthreshold, Pthreshold=Pthreshold)
	return() 

out3=oneTrading(market="BTC-GRS",num_windows=12,refresh_frequency=1, Vthreshold=0.5, Pthreshold=0.05, 
thre_drop=0.01,recent_days=0.1)




#next, save return to a local folder 

	#1. read all market data: pass the ones not wanted 
	#2. for each trading pair, process data, save return and plot 
	


























#Q2: weifeng's code; 29L ,
def getListOfMarket(rawMarketData):
    listOfMarket = list()
    for record in rawMarketData['result']:
        tradingPair = record[MARKETNAME]
        if (tradingPair.startswith('BTC')):
            listOfMarket.append(record[MARKETNAME])
    
    return listOfMarket 
def retrieveMarketHistoricalData(rawMarketData):
    marketHistoricalData = dict()
    print('Total number of market is {}'.format(str(len(rawMarketData['result']))))
    listOfMarket = getListOfMarket(rawMarketData)
    
    timeStop = str(datetime.now() - timedelta(hours = HOURINTEREST)).replace(' ', 'T')
    
    marketLimit = int(MARKETLIMIT)
    for market in listOfMarket:
        print(market)
        individualMarketUrl = INDIVIDUALSUMMARYPREFIX + market + INDIVIDUALSUMMARYPOSTFIX
        unfilledData = json.loads(urlopen(individualMarketUrl).read())['result']

        try:
            it = next(i for i in xrange(len(unfilledData)) if unfilledData[i]['T'] >= timeStop)
        except:
            print(market + ": no valid data within last " + str(HOURINTEREST) + " hours")
        else:
            cutUnfilledData = unfilledData[it:]
            marketHistoricalData[market] = cutUnfilledData
        finally:
            # Need to break before running limit
            marketLimit = marketLimit - 1
            if (marketLimit <= 0):
                break
    
    return marketHistoricalData
    
    
    
    
    


col_name=['holding_status']	
df[col_name].plot()
	
col_name=['buy_new']
df[col_name].plot()

#debug, nothing wrong, just rerun it; 

df.loc[lambda df: df.holding_status < 0, :]
df.index.get_loc('2017-08-18 07:45:00')

col_names=['buy_ind','buy_new', 'sell_ind', 'holding_status']
df_new=df[col_names]

df_new[290:305]

##########################
#test stance idea , not the same as using 'signal'

x=0

df['Stance']=np.where( (df['C'] - sma20) > x, 1, 0)

df['Stance']=np.where( (df['C'] - sma20) < x, -1, df['Stance'])

df['Stance'].value_counts()

a=df['signal_20']-df['signal_20'].shift(1)

a.fillna().value_counts() 

df['strategy_stance']=df['returns']*df['Stance'].shift(1)

col_name=['returns', 'mystrategy_20', 'strategy_stance']

df[col_name].dropna().cumsum().apply(np.exp).plot()




#################
#code code , have bugs in it. 
cols = []

# position
for momentum in [15]: 
	col = 'position_%s' % momentum
    #df[col] = np.sign(df['returns'].rolling(momentum,min_periods=momentum).mean())
    df[col] = np.sign(df['returns'].rolling(momentum,min_periods=momentum).mean()) #here 
    cols.append(col)
    
%matplotlib inline


import seaborn as sns

sns.set()

#strategy
strats = ['returns']

for col in cols:
    strat = 'strategy_%s' % col.split('_')[1]
    df[strat] = df[col].shift(1) * df['returns']
    strats.append(strat)

df[strats].dropna().cumsum().apply(np.exp).plot()
plt.show()


######################
#bt backtest example, code from http://pmorissette.github.io/bt/examples.html
#We can use bt for single and portfolio trading pairs 
import bt

data=df[['C']]
sma = data.rolling(20).mean()
plot = bt.merge(data, sma).plot(figsize=(15, 5))
plt.show()
signal = data > sma
# first we create the Strategy
s = bt.Strategy('above2hoursma', [bt.algos.SelectWhere(data > sma), #do not replace with df['C'] or sma20
                               bt.algos.WeighEqually(),
                               bt.algos.Rebalance()])

# now we create the Backtest
t = bt.Backtest(s, data)


s_long = bt.Strategy('long', [bt.algos.RunOnce(),
                           bt.algos.SelectAll(),
                           bt.algos.WeighEqually(),
                           bt.algos.Rebalance()])

t_long=bt.Backtest(s_long,data)
                         
# and let's run it!
res = bt.run(t,t_long)
# what does the equity curve look like?
res.plot()  #here, add price to this? later 
res.plot_security_weights()
# and some performance stats
res.display()


############################
#try portfolio code from https://s3.amazonaws.com/quantstart/media/powerpoint/an-introduction-to-backtesting.pdf
#code for portfolio; buy 100 shares each time, not applicable; 
df['signal']=(data>sma) 

df2=df[['C','signal']]

df2['position']=df2['signal']-df2['signal'].shift(1)

df2['portfolio']=df2['position']*df2['C']

df2['pos_diff']=df2['position']-df2['position'].shift(1)

df2['holdings']=(df2['position']*df2['C']).sum(axis=1)

#from internet 
portfolio['holdings'] = (self.positions*self.bars['Adj Close']).sum(axis=1)
 portfolio['cash'] = self.initial_capital - (pos_diff*self.bars['Adj Close']).sum(axis=1).cumsum()
 # Sum up the cash and holdings to create full account ‘equity’, then create the percentage returns
 portfolio['total'] = portfolio['cash'] + portfolio['holdings']
 portfolio['returns'] = portfolio['total'].pct_change()
 
















#old code; example to use bt 
def above_sma(tickers, sma_per=50, start='2010-01-01', name='above_sma'):
    """
    Long securities that are above their n period
    Simple Moving Averages with equal weights.
    """
    # download data
    data = bt.get(tickers, start=start)
    # calc sma
    sma = data.rolling(sma_per).mean()

    # create strategy
    s = bt.Strategy(name, [SelectWhere(data > sma),
                           bt.algos.WeighEqually(),
                           bt.algos.Rebalance()])

    # now we create the backtest
    return bt.Backtest(s, data)

# simple backtest to test long-only allocation
def long_only_ew(tickers, start='2010-01-01', name='long_only_ew'):
    s = bt.Strategy(name, [bt.algos.RunOnce(),
                           bt.algos.SelectAll(),
                           bt.algos.WeighEqually(),
                           bt.algos.Rebalance()])
    data = bt.get(tickers, start=start)
    return bt.Backtest(s, data)

# create the backtests
#tickers = 'msft'
#sma10 = above_sma(tickers, sma_per=10, name='sma10')
#sma20 = above_sma(tickers, sma_per=20, name='sma20')
#sma40 = above_sma(tickers, sma_per=40, name='sma40')
benchmark = long_only_ew('spy', name='spy')

# run all the backtests!
res2 = bt.run(sma10, benchmark)
res2.plot(freq='m')

res2.display()












#install any new package 
#install pandas in terminal: application 
#pip install wheel
pip install pandas

