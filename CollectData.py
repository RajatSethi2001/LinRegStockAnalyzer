import requests
import time
import re
import copy
import os
import asyncio
import aiohttp
import numpy as np
from datetime import datetime

date = datetime.today().strftime('%m-%d-%y')
backupFile = open(f"training-data/backup_{date}.txt", "w+")
nasdaqFile = open("nasdaqFile.txt", "w+")
nyseFile = open("nyseFile.txt", "w+")
os.system("bash ./nasdaq.sh > nasdaqFile.txt")
os.system("bash ./nyse.sh > nyseFile.txt")

nasdaqList = nasdaqFile.readlines()
nyseList = nyseFile.readlines()
symbolDict = {}
symbolNum = 0

for line in range(len(nasdaqList)):
    nasdaqList[line] = nasdaqList[line].split(" ")
    symbolDict[nasdaqList[line][0]] = "NASDAQ"

for line in range(len(nyseList)):
    nyseList[line] = nyseList[line].split(" ")
    if (nyseList[line][2] == "N"):
        symbolDict[nyseList[line][0]] = "NYSE"

async def main(symbolDict):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for ticker in symbolDict.keys():
            url = f"https://www.marketbeat.com/stocks/{symbolDict[ticker]}/{ticker}/price-target/"
            task = asyncio.create_task(fetch(session, url, ticker))
            tasks.append(task)

        await asyncio.gather(*tasks)

async def fetch(session, url, ticker):
    try:
        async with session.get(url) as resp:
            text = await resp.text()
            await scrape(text, ticker)
    except:
        pass

async def scrape(text, ticker):
    global symbolNum
    try:
        MarketSource = text.replace("\n","").replace(" ","").replace(",","")
        price = float(re.search("'price'><strong>\$([0-9]+\.[0-9]*)</strong>", MarketSource).group(1))
        volumeStr = re.search('>Volume<strong>([0-9]+(\.)*[0-9]*(s|m|b|t))', MarketSource).group(1)
        multiFactor = 1
        if (volumeStr[-1] == 't'):
            multiFactor = 1000000000000
        elif(volumeStr[-1] == 'b'):
            multiFactor = 1000000000
        elif(volumeStr[-1] == 'm'):
            multiFactor = 1000000
        volume = float(volumeStr[0:len(volumeStr)-1]) * multiFactor

        avgVolumeStr = re.search('AverageVolume<strong>([0-9]+(\.)*[0-9]*(s|m|b|t))', MarketSource).group(1)
        multiFactor = 1
        if (avgVolumeStr[-1] == 't'):
            multiFactor = 1000000000000
        elif(avgVolumeStr[-1] == 'b'):
            multiFactor = 1000000000
        elif(avgVolumeStr[-1] == 'm'):
            multiFactor = 1000000
        avgVolume = float(avgVolumeStr[0:len(avgVolumeStr)-1]) * multiFactor

        marketCapStr = re.search('MarketCapitalization<strong>\$([0-9]+\.[0-9]*(m|b|t))', MarketSource).group(1)
        multiFactor = 0
        if (marketCapStr[-1] == 't'):
            multiFactor = 1000000000000
        elif(marketCapStr[-1] == 'b'):
            multiFactor = 1000000000
        elif(marketCapStr[-1] == 'm'):
            multiFactor = 1000000
        marketCap = int(float(marketCapStr[0:len(marketCapStr)-1]) * multiFactor)
        
        try:
            PERatio = float(re.search('P/ERatio<strong>([0-9]+\.[0-9]*)', MarketSource).group(1))
        except:
            PERatio = 0

        try:
            dividendYield = float(re.search('DividendYield<strong>([0-9]+\.[0-9]*)%', MarketSource).group(1))
        except:
            dividendYield = 0
        
        beta = float(re.search('Beta<strong>([0-9]+\.[0-9]*)', MarketSource).group(1))
        lowcast = round(float(re.search(f"LowPT</th><tdclass='text-right'>\$([0-9]+\.[0-9]*)", MarketSource).group(1)) * 100/price - 100, 2)          
        forecast = round(float(re.search(f"AveragePT</th><tdclass='text-right'>\$([0-9]+\.[0-9]*)", MarketSource).group(1)) * 100/price - 100, 2)
        highcast = round(float(re.search(f"HighPT</th><tdclass='text-right'>\$([0-9]+\.[0-9]*)", MarketSource).group(1)) * 100/price - 100, 2)

        CurrentRatings = re.search("bg\-dark\-green(.*)ConsensusPriceTarget",MarketSource).group(1)
        CurrentRatingsList = re.findall(">([0-9]+|N/A)<", CurrentRatings)
        Buy = int(CurrentRatingsList[0])
        Overweight = int(CurrentRatingsList[4])
        Hold = int(CurrentRatingsList[8])
        Sell = int(CurrentRatingsList[12])
        analysts = Buy + Overweight + Hold + Sell

        Sentiment = re.search("ItsCompetitors(.*)AnalystRatingsHistory",MarketSource).group(1)
        SentimentList = re.findall(">([0-9]+)<",Sentiment)
        Outperform = int(SentimentList[0])
        Underperform = int(SentimentList[1])
        NewsSentiment = re.findall("(Positive|Neutral|Negative)",Sentiment)[0]
        
        data = (f"{ticker},{date},{price},{volume},{avgVolume},{marketCap},{PERatio},{dividendYield},{beta},{NewsSentiment},{Outperform},{Underperform},{analysts},{Buy},{Overweight},{Hold},{Sell},{lowcast},{forecast},{highcast}")
        backupFile.write(f"{data}\n")
        symbolNum += 1
        print(f"{symbolNum}: {data}")

    except:
        pass

asyncio.run(main(symbolDict))  
backupFile.close()

