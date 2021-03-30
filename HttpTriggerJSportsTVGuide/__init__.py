import logging

import azure.functions as func

from datetime import datetime, timedelta, time, date
import requests
from bs4 import BeautifulSoup as BS
import pytz
import pandas as pd
from MyFunctions import (
    create_insert_query,
    run_sql_commmand,
    create_removeDuplicates_query
)

def analyseDict(dicto):
    return {i : list(dicto[i].values()).count(True)
            for i in range(1,5)
                }
        

def sortTimeList(timeList0):
    """Sort list where 4am is min and 3.59am is max"""
    timeList = sorted(timeList0)
    ## Split into pre 4am (effectively tomorrow) and post 4am
    pre4am = [x for x in timeList if x >= time(hour=4)]
    post4am = [x for x in timeList if x < time(hour=4)]
    return pre4am + post4am

def getEarliestUnaccountedMinuteChannels(accountedMinutes):
    mins = []
    minsANDchans = []
    for i in range(1,5):
        ## Get all False minutes for the chan and order them
        falseMins = [t for t,tf in accountedMinutes[i].items() if tf == False]
        falseMins.sort()
        if len(falseMins) > 0:
            ## Concat and get first minute
            firstUnaccountedMin = sortTimeList(falseMins)[0]
            ## Add to lists
            mins.append(firstUnaccountedMin)
            minsANDchans.append((firstUnaccountedMin,i))
    ## Sort and get first value (effectively the earliest value)
    minsSorted = sortTimeList(mins)
    if len(minsSorted) != 0:
        earliestMin = minsSorted[0]
        ## Get all the channels with that same earliestMin
        return [y for x,y in minsANDchans if x == earliestMin],earliestMin
    else:
        return [],None

def scrape_jsports():    
    ## Create dict of dicts to record all the minutes accounted for
    accountedMinutes = {}
    for i in range(1,5):
        dicto = {
                (datetime.combine(date=datetime.now(),
                                time=time(hour=0)) + timedelta(minutes=j)).time() : False
                for j in range(24*60)
                }
        accountedMinutes[i] = dicto


    utc_tz = pytz.timezone("utc")
    jp_tz = pytz.timezone("Asia/Tokyo")

    ## Get tomorrow's date in right format
    dateToUse = datetime.now()
    dateString = dateToUse.strftime("%y%m%d")

    ## Send request
    req = requests.get(f"https://www.jsports.co.jp/program_guide/month/english/{dateString}")
    ## Get soup
    soup = BS(req.text,'html.parser')
    tbody = soup.find('tbody')
    trs = tbody.find_all('tr')
    progs = []


    for I,tr in enumerate(trs):
        tds = [
            x
            for x in tr.find_all('td')#,attrs={'class':"w-channel__item"})
            if x.attrs['class'][0] in ["w-channel__item","w-channel__item--now"]
        ]
        
        ## If no tds, skip to next tr
        if len(tds) == 0:
            continue
        
        ## If there are 4 <td> elements, there's one for each channel
        all4 = len(tds) == 4
        if all4:
            channelList = [f"J Sports {i}" for i in range(1,5)]
        ## If there aren't, work out which the channels are
        else:
            geumc,earliestMin = getEarliestUnaccountedMinuteChannels(accountedMinutes)
            assert len(geumc) == len(tds)
            channelList = [f"J Sports {i}" for i in geumc]
        ## Get progs
        for i,td in enumerate(tds):
            tba = {}
            ## Starts and Ends
            try:
                ## 'pm0:00' is used rather than 'pm12:00', correct their mistake
                txt = td.p.text.replace("pm0:","pm12:").replace("am0:","am12:")
                T = datetime.strptime(txt,"%p%I:%M").time()
            except AttributeError:
                if I == 0:
                    T = time(hour=4)
                else:
                    raise ValueError("no time provided")
            dtu = dateToUse \
                    if (T >= time(hour=0)) & (T <= time(hour=4)) \
                    else dateToUse + timedelta(days=1)
            tba['StartLocal'] = datetime.combine(
                                    date=dtu,
                                    time=T
                                )
            tba['StartUTC'] = jp_tz.localize(
                                    tba['StartLocal']
                                        ).astimezone(
                                                utc_tz
                                                    ).replace(tzinfo=None)
            durationMins = int(td.attrs['rowspan'])
            tba['EndLocal'] = tba['StartLocal'] + timedelta(minutes=durationMins)
            tba['EndUTC'] = tba['StartUTC'] + timedelta(minutes=durationMins)
            ## Channel
            tba['Channel'] = channelList[i]
            channelNumber = int(channelList[i][-1])
            ## ProgrammeName
            tba['ProgrammeName'] = td.dd.text.strip()
            
            progs.append(tba)
            
            T2 = (datetime.combine(
                        date=datetime.now(),
                        time=T) + timedelta(minutes=durationMins)).time()
            for m in range(durationMins):
                accountedMin = (datetime.combine(
                                    date=datetime.now(),
                                    time=T) + timedelta(minutes=m)).time()
                accountedMinutes[channelNumber][accountedMin] = True
        
    DF = pd.DataFrame(progs)

    columnDict = {
        "StartLocal" : 'DateTime',
        "EndLocal" : 'DateTime',
        "StartUTC" : 'DateTime',
        "EndUTC" : 'DateTime',
        "Channel" : 'str',
        "ProgrammeName" : 'str'
        }
    server = "nonDashboard"
    database = "WebScraping"
    sqlTableName = "JSportsTVGuide"
    primaryKeyColName = "RowID"
        
    insertQ = create_insert_query(DF,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

    removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

    run_sql_commmand(removeDuplicatesQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_jsports()

    logging.info("scraping done")

    return func.HttpResponse(f"Done")
