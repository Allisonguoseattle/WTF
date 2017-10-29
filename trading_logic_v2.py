from __future__ import print_function

import os
import json
import boto3
from datetime import datetime, timedelta
from urllib2 import urlopen
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

SITE = os.environ['site']  # URL of the site to check, stored in the site environment variable, e.g. https://aws.amazon.com
EXPECTED = os.environ['expected']  # String expected to be on the page, stored in the expected environment variable, e.g. Amazon
MARKETNAME = os.environ['marketName']
TIMESTAMP = os.environ['timeStamp']
TRADINGSNS = os.environ['tradingSNS']
INDIVIDUALSUMMARYPREFIX = os.environ['individualSummaryPrefix']
INDIVIDUALSUMMARYPOSTFIX = os.environ['individualSummaryPostfix']
MARKETLIMIT = os.environ['marketLimit']
HOURINTEREST = int(os.environ['hourInterest'])

dynamodb = boto3.resource('dynamodb')
holdingStatusTable = dynamodb.Table('MarketHoldingStatus')

def validateBittrex(rawMarketData):
    checkResult = rawMarketData['success']
    return EXPECTED == str(checkResult)

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
    
def getListOfMarket(rawMarketData):
    listOfMarket = list()
    for record in rawMarketData['result']:
        tradingPair = record[MARKETNAME]
        if (tradingPair.startswith('BTC')):
            listOfMarket.append(record[MARKETNAME])
    
    return listOfMarket

def setHoldingStatus(marketName, holdingStatus, buyPrice, peakPrice):
    newPeakPrice = str()
    newBuyPrice = str()
    if (holdingStatus == 'False'):
        newPeakPrice = '0'
        newBuyPrice = '0'
    else:
        currentHoldingStatus = getHoldingStatus(marketName)
        
        if (currentHoldingStatus is not None):
            # Update peak Price
            if (float(currentHoldingStatus['PeakPrice']) >= peakPrice):
                newPeakPrice = currentHoldingStatus['PeakPrice']
            else:
                newPeakPrice = str(peakPrice)
            # Will not update buy price
            if (currentHoldingStatus['BuyPrice'] == '0'):
                newBuyPrice = str(buyPrice)
            else:
                newBuyPrice = currentHoldingStatus['BuyPrice']
        else:
            newBuyPrice = str(buyPrice)
            newPeakPrice = str(peakPrice)
            
    holdingStatusTable.put_item(
            Item = {
                'MarketName': marketName,
                'TimeStamp': str(datetime.now()),
                'HoldingStatus': holdingStatus,
                'BuyPrice': newBuyPrice,
                'PeakPrice': newPeakPrice
            }
        )

def getHoldingStatus(marketName):
    response = holdingStatusTable.query(
            KeyConditionExpression=Key(MARKETNAME).eq(marketName)
        )
        
    print(str(len(response['Items'])))
    if (len(response['Items']) != 1):
        return None
    else:
        return response['Items'][0]

def updatePeakPrice(marketHistoricalData):
    timeStop = str(datetime.now() - timedelta(minutes = 55)).replace(' ', 'T')
    response = holdingStatusTable.scan(
            FilterExpression=Key('HoldingStatus').eq('True')
        )
        
    for holdingPair in response['Items']:
        pair = holdingPair[MARKETNAME]
        historicalData = marketHistoricalData[pair]
        potentialPeakPrice = None
        
        for data in (data for data in historicalData if data['T'] >= timeStop):
            if (potentialPeakPrice is None):
                potentialPeakPrice = data['C']
            elif (potentialPeakPrice < data['C']):
                potentialPeakPrice = data['C']
        
        if (potentialPeakPrice is not None):
            print(pair + ' with new peak price: ' + str(potentialPeakPrice))
            setHoldingStatus(pair, 'True', potentialPeakPrice, potentialPeakPrice)
            
    return
    
def triggerTradingSNS(buyingCandidates, sellingCandidates):
    sns = boto3.client(service_name="sns")
    topicArn = TRADINGSNS
    print('Trading is triggered!')
    print(buyingCandidates)
    print(sellingCandidates)
    sns.publish(
        TopicArn = topicArn,
        Message = 'Trading is triggered!' + 'BuyingCandidates: ' + str(buyingCandidates) + 'SellingCandidates: ' + str(sellingCandidates)
    )
    return

def sellExecution(sellingCandidates):
    for candidate in sellingCandidates:
        print('Start to sell ' + str(candidate[1]['pair']))
        setHoldingStatus(candidate[1]['pair'], 'False', 0, 0)

    return

def buyExecution(buyingCandidates):
    for candidate in buyingCandidates:
        print('Start to buy ' + str(candidate[1]['pair']))
        # This buying price will be based on the time of execution
        buyPrice = 0
        setHoldingStatus(candidate[1]['pair'], 'True', buyPrice, buyPrice)
    
    return









def buySig(tradingPair,currPrice,prePrice,currRWVolumeSum,preRWVolumeSum,twentyFourHourBTCVolume,weights={'V':0.8,'P':0.2},thresholds={'V':1,'P':0.05,'twentyFourHourBTCVolume':300}):
	if currPrice==None or prePrice==None or twentyFourHourBTCVolume==None:
		print(currPrice)
		print(prePrice)
		print(twentyFourHourBTCVolume)
		raise ValueError('erroneous currPrice OR prePrice OR twentyFourHourBTCVolume')
	if currRWVolumeSum==None or preRWVolumeSum==None or currRWVolumeSum<=0 or preRWVolumeSum<=0:
		raise ValueError()
	if sum(weights.values())!=1:
		raise ValueError('weights must be sum to 1')
	if thresholds==None:
		raise ValueError('threshold: '+str(thresholds))
	# if currPrice<prePrice:
	# 	print(tradingPair+' has a lower price (curr:'+str(currPrice)+') vs (pre:'+str(prePrice)+')')
	# 	return None
	if twentyFourHourBTCVolume<thresholds['twentyFourHourBTCVolume']:
		print(tradingPair+' twentyFourHourBTCVolume < '+str(thresholds['twentyFourHourBTCVolume']))
		return None
	vThresholdValue=(currRWVolumeSum-preRWVolumeSum)/preRWVolumeSum
	pThresholdValue=(currPrice-prePrice)/prePrice
	if vThresholdValue<thresholds['V']:
		print(tradingPair+' not passing Volume threshold ('+str(vThresholdValue)+' vs '+str(thresholds['V'])+')')
		return None
	if pThresholdValue<thresholds['P']:
		print(tradingPair+' not passing price threshold ('+str(pThresholdValue)+' vs '+str(thresholds['P'])+')')
		return None
	return vThresholdValue/thresholds['V']*weights['V']+pThresholdValue/thresholds['P']*weights['P']


def sellSig(holdingStatus,currPrice,thresholds={'stopLoss':-0.07,'stopPeakLoss':-0.1,'stopGain':0.2}):
	#{u'TimeStamp': u'2017-09-30 19:45:20.873574', u'HoldingStatus': u'False', u'MarketName': u'BTC-1ST', u'PeakPrice': u'0', u'BuyPrice': u'0'}
	import sys
	if holdingStatus==None or holdingStatus['HoldingStatus']=='False':
		return None
	if holdingStatus['BuyPrice']==None or currPrice==None or thresholds==None:
		raise ValueError('erroneous holdingStatus('+str(holdingStatus['BuyPrice'])+') OR currPrice('+str(currPrice)+') OR thresholds('+str(thresholds)+')')
	holdingStatus['BuyPrice']=float(holdingStatus['BuyPrice'])
	holdingStatus['PeakPrice']=float(holdingStatus['PeakPrice'])
	if holdingStatus['BuyPrice']<0 or currPrice<0:
		raise ValueError('erroneous holdingStatus('+str(holdingStatus)+') OR currPrice('+str(currPrice)+')')
	if (currPrice-holdingStatus['BuyPrice'])/holdingStatus['BuyPrice']<=thresholds['stopLoss']:
		return sys.maxint
	if (currPrice-holdingStatus['PeakPrice'])/holdingStatus['PeakPrice']<=thresholds['stopPeakLoss']:
		return sys.maxint
	# if (currPrice-holdingStatus['BuyPrice'])/holdingStatus['BuyPrice']>=thresholds['stopGain']:
	# 	return sys.maxint
	return None



def rollingWindow(tradingPair,data,histTimeInterval=1,rwLength=60,checkTimeInterval=5,warningTimeGap=60,maxLatency=5,lastVCheckTimeSpan=5,lastPCheckTimeSpan=5,lastPVCheckThreshold={'p':0,'v':30}):
	#-------------------------------
	#this function is used to deal with singal trading pair, e.g. bit-omg
	#the time units for rwLength and checkTimeInterval and inputTimeInterval are min 
	#we are assuming input data is a list of json object
	#this is following https://docs.google.com/document/d/1XCX_g96ro82I-nFQC6RHXKQkDu2uP1WrXbPvD64qe54/edit#
	#fixed check interval without smoothing will result in very volatile signals
	#-------------------------------
	import datetime
	import time
	#import numpy as np
	#import pandas as pd
	#import collections as c
	#basic sanity check
	if tradingPair==None:
		raise ValueError("erroneous tradingPair: "+str(tradingPair))
	if data==None or len(data)<=5:
		#here need to check with sell logic, for that if data==None, which means we dont have this pair's history, but this doesn't mean it's not trading (due to lag or anything else), if this's the case we may lose the sell signal
		raise ValueError("erroneous input data: "+str(data))
	#sort data to make sure its time ascending
	data.sort(key=lambda x:x['T'])
	print('latest timeStamp: '+str(tradingPair)+' '+str(data[-1]['T']))
	#check sell signal before everything else
	currPrice,currTS=data[-1]['C'],time.mktime(datetime.datetime.strptime(data[-1]['T'],"%Y-%m-%dT%H:%M:%S").timetuple())
	#read holding position here
	holdingStatus=getHoldingStatus(tradingPair)
	sellSignal=sellSig(holdingStatus=holdingStatus,currPrice=currPrice,thresholds={'stopLoss':-0.07,'stopPeakLoss':-0.1,'stopGain':1000})

	if warningTimeGap==None or (not 0<warningTimeGap):
		raise ValueError('warningTimeGap >0')
	if histTimeInterval>=warningTimeGap:
		raise ValueError('histTimeInterval: '+str(histTimeInterval)+'must be less than warningTimeGap: '+str(warningTimeGap))
	if lastVCheckTimeSpan==None or lastVCheckTimeSpan<0 or lastVCheckTimeSpan>1440:
		raise ValueError('erroneous lastVCheckTimeSpan: '+str(lastVCheckTimeSpan))
	if lastPCheckTimeSpan==None or lastPCheckTimeSpan<0 or lastPCheckTimeSpan>1440:
		raise ValueError('erroneous lastPCheckTimeSpan: '+str(lastPCheckTimeSpan))
	if lastPVCheckThreshold==None:
		raise ValueError('erroneous lastPVCheckThreshold')
	if maxLatency==None or maxLatency>6:
		raise ValueError('None maxLatency or maxLatency('+str(maxLatency)+') cannot exceed 6min due to dynamic last timeStamp')
	if time.time()-currTS>maxLatency*60:
		print('warning: '+str(tradingPair)+' last update timestamp too old: '+str(data[-1]['T']))
		return {'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
	if lastPCheckTimeSpan<maxLatency or lastVCheckTimeSpan<maxLatency:
		print('warning: lastPCheckTimeSpan('+str(lastPCheckTimeSpan)+') or lastVCheckTimeSpan('+str(lastVCheckTimeSpan)+') is less than maxLatency('+str(maxLatency)+') which means trading pairs which last entry satisfying (currentTime-maxLatency <= timeStamp < currentTime-last[P,V]CheckTimeSpan) will automatically fail last min checks')
	if time.mktime(datetime.datetime.strptime(data[-1]['T'],"%Y-%m-%dT%H:%M:%S").timetuple())-time.mktime(datetime.datetime.strptime(data[0]['T'],"%Y-%m-%dT%H:%M:%S").timetuple())<86400:
		print('history not exceeding 24h'+str(data[-1]['T'])+' '+str(data[0]['T']))
		return {'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}

	#initialization
	prePrice=None
	currRWtimeFrame,preRWtimeFrame={'start':currTS-rwLength*60,'end':currTS},{'start':currTS-checkTimeInterval*60-rwLength*60,'end':currTS-checkTimeInterval*60}
	currRWtimeWriteFlag,preRWtimeWriteFlag=False,False
	stopTime=currRWtimeFrame['end']-86400
	currRWVolumeSum,preRWVolumeSum,twentyFourHourBTCVolume=0,0,0
	preTs=None
		#last X min check
	lastMinCheck=True
	lastV,lastP=0,None
	lastVtimeFrame={'start':currRWtimeFrame['end']-lastVCheckTimeSpan*60,'end':currRWtimeFrame['end']}


	for i in range(len(data)-1,-1,-1):
		ts=time.mktime(datetime.datetime.strptime(data[i]['T'],"%Y-%m-%dT%H:%M:%S").timetuple())
		if preTs!=None:
			if preTs-ts>warningTimeGap*60:
				print('warning, '+str(tradingPair)+' time interval exceeds warningTimeGap('+str(warningTimeGap)+') '+str(data[i]['T'])+' '+str(data[i+1]['T']))
				return {'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
			if preTs-ts<histTimeInterval*60:
				print(str(data[i-1]))
				print(str(data[i]))
				print('data timestamp overlapping, will skip this trading pair')
				return {'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
		if ts<stopTime:
			break
		if ts>currRWtimeFrame['end']:
			print('warning: data last time stamp('+str(data[i]['T'])+') is larger than current time stamp('+str(currRWtimeFrame['end'])+')')
			return {'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
		if prePrice==None and ts<=currRWtimeFrame['end']-rwLength*60:
			prePrice=data[i]['C']
		if lastMinCheck:
			if lastP==None and ts<=currRWtimeFrame['end']-lastPCheckTimeSpan*60:
				lastP=data[i]
			if ts>=lastVtimeFrame['start']:
				lastV+=data[i]['V']*data[i]['C']
			else:
				if lastP==None:
					pass
				elif currPrice-lastP['C']>lastPVCheckThreshold['p'] and lastV>lastPVCheckThreshold['v']:
					lastMinCheck=False
				else:
					print('warning: tradingPair '+str(tradingPair)+' not passing last min checks (lastPrice:'+str(lastP)+' currPrice:'+str(data[-1])+' vs lastPriceThreshold:'+str(lastPVCheckThreshold['p'])+', lastVolume:'+str(lastV)+' vs lastVolumeThreshold:'+str(lastPVCheckThreshold['v'])+')')
					print('lastVCheckTimeSpan: '+str(lastVCheckTimeSpan)+'min, lastPCheckTimeSpan: '+str(lastPCheckTimeSpan)+'min')
					return {'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
		if currRWtimeFrame['start']<=ts<=currRWtimeFrame['end']:
			currRWVolumeSum+=data[i]['V']
			currRWtimeWriteFlag=True
		if preRWtimeFrame['start']<=ts<=preRWtimeFrame['end']:
			preRWVolumeSum+=data[i]['V']
			preRWtimeWriteFlag=True
		if stopTime<=ts<=currRWtimeFrame['end']:
			twentyFourHourBTCVolume+=data[i]['V']*data[i]['C']
		preTs=ts

	if not (currRWtimeWriteFlag and preRWtimeWriteFlag):
		print(currRWtimeFrame,preRWtimeFrame,stopTime)
		print(data[:5])
		print(data[-5:])
		print('not writing, currRWVolumeSum: '+str(currRWVolumeSum)+', preRWVolumeSum: '+str(preRWVolumeSum))
		return {'buySig':None,'sellSig':sellSignal,'twentyFourHourBTCVolume':None,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}
	return {'buySig':buySig(tradingPair=tradingPair,currPrice=currPrice,prePrice=prePrice,currRWVolumeSum=currRWVolumeSum,preRWVolumeSum=preRWVolumeSum,twentyFourHourBTCVolume=twentyFourHourBTCVolume,weights={'V':0.8,'P':0.2},thresholds={'V':0.5,'P':0.025,'twentyFourHourBTCVolume':0}),'sellSig':sellSignal,'twentyFourHourBTCVolume':twentyFourHourBTCVolume,'peakPrice':(holdingStatus['PeakPrice'] if holdingStatus!=None else None),'buyPrice':(holdingStatus['BuyPrice'] if holdingStatus!=None else None),'currPrice':currPrice}



def generateCandidates(marketHistoricalData):
	import heapq as hq
	import time
	if marketHistoricalData==None:
		raise ValueError('erroneous marketHistoricalData')
	buyCand,sellCand=[],[]
	for pair in marketHistoricalData.keys():
		ans=rollingWindow(tradingPair=pair,data=marketHistoricalData[pair],histTimeInterval=1,rwLength=60,checkTimeInterval=5,warningTimeGap=40,maxLatency=5,lastVCheckTimeSpan=5,lastPCheckTimeSpan=5,lastPVCheckThreshold={'p':0,'v':10})
		if ans!=None and ans['buySig']!=None:
			hq.heappush(buyCand,(-ans['buySig'],{'pair':pair,'twentyFourHourBTCVolume':ans['twentyFourHourBTCVolume'],'peakPrice':ans['peakPrice'],'buyPrice':ans['buyPrice'],'currPrice':ans['currPrice'],'currentTS':time.time()}))
		if ans!=None and ans['sellSig']!=None:
			hq.heappush(sellCand,(-ans['sellSig'],{'pair':pair,'twentyFourHourBTCVolume':ans['twentyFourHourBTCVolume'],'peakPrice':ans['peakPrice'],'buyPrice':ans['buyPrice'],'currPrice':ans['currPrice'],'currentTS':time.time()}))
	return (buyCand,sellCand)






# item=heapq.heappop(buyCand)
# -item[0] is the score
# item[1] is the dict that containing this trading pair's info

def lambda_handler(event, context):
    marketUrl = SITE
    try:
        print(marketUrl)
        rawMarketData = json.loads(urlopen(marketUrl).read())
        if not validateBittrex(rawMarketData):
            raise Exception('Validation failed')
            
        # RetrieveMarketHistoricalData
        marketHistoricalData = retrieveMarketHistoricalData(rawMarketData)
        
        # Update peak price for holding pairs
        updatePeakPrice(marketHistoricalData)
        
        # GenerateTradingCandidates
        buyingCandidates,sellingCandidates = generateCandidates(marketHistoricalData)
        print('buyingCandidates:',buyingCandidates)
        print('sellingCandidates:',sellingCandidates)
        if (len(buyingCandidates) != 0) or (len(sellingCandidates) != 0):
            triggerTradingSNS(buyingCandidates, sellingCandidates)
        
        # Selling
        sellExecution(sellingCandidates)
        
        # Buying
        buyExecution(buyingCandidates)
    except:
        print('Check failed!')
        raise
    else:
        print('Check passed!')
        return event['time']
    finally:
        print('Check complete at {}'.format(str(datetime.now())))
