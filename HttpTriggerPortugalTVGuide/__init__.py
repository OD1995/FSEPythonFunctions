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

def scrape_portugal():    
    utc_tz = pytz.timezone("utc")
    por_tz = pytz.timezone("Europe/Lisbon")
    ## List of channels to loop through
    ch = [
        (532,"Eleven Sports 3"),
        (514,"Eleven Sports 4")
    ]
    progs = []
    for channelID,channelName in ch:
        url = f"https://www.nos.pt/particulares/televisao/guia-tv/Pages/channel.aspx?channel={channelID}"
        todayDate = datetime.now().date()
        req = requests.get(url)
        soup = BS(req.text,'html.parser')

        ## Get first column (today's column)
        firstCol = soup.find("div",attrs={'class':["programs-day-list","active-day"]})
        boxes = firstCol.find_all('li')
        for i,li in enumerate(boxes):
            tba = {}
            ## Channel
            tba['Channel'] = channelName
            ## ProgrammeName
            tba['ProgrammeName'] = li.a['title']
            ## Start & End
            seText = li.find('span',attrs={'class':'duration'}).text.strip()
            for punc in ["\r","\n"," "]:
                seText = seText.replace(punc,"")
            start,end = seText.split("-")
            startT = datetime.strptime(start,"%H:%M").time()
            endT = datetime.strptime(end,"%H:%M").time()
            ## If first start time is yesterday, adjust for that
            if (i == 0) & (startT.hour > 12):
                startDT = datetime.combine(
                            date=todayDate - timedelta(days=1),
                            time=startT
                    )
            else:
                startDT = datetime.combine(
                            date=todayDate,
                            time=startT
                    )
            endDT = datetime.combine(
                            date=todayDate,
                            time=endT
                    )
            tba['StartLocal'] = startDT
            tba['EndLocal'] = endDT
            tba['StartUTC'] = por_tz.localize(
                                        tba['StartLocal']
                                            ).astimezone(
                                                    utc_tz
                                                        ).replace(tzinfo=None)
            tba['EndUTC'] = por_tz.localize(
                                        tba['EndLocal']
                                            ).astimezone(
                                                    utc_tz
                                                        ).replace(tzinfo=None)
            progs.append(tba)
        
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
    sqlTableName = "PortugalTVGuide"
    primaryKeyColName = "RowID"
        
    insertQ = create_insert_query(DF,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

    removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

    run_sql_commmand(removeDuplicatesQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_portugal()

    logging.info("scraping done")

    return func.HttpResponse(f"Done")
