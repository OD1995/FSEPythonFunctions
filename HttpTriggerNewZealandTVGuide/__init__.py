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

def scrape_nz():    
    utc_tz = pytz.timezone("utc")
    nz_tz = pytz.timezone("Pacific/Auckland")

    today9am = int(
            datetime.combine(
                date=datetime.now(),
                time=time(hour=9)
            ).timestamp()*1e3)
    tomorrow859am = int(
            datetime.combine(
                date=datetime.now()+timedelta(days=1),
                time=time(hour=8,minute=59)
            ).timestamp()*1e3)

    ## List of channels to scrape, case sensitive
    channels = [
            'SKY Sport Select'
            ,'SKY Sport 1'
            ,'SKY Sport 2'
            ,'SKY Sport 3'
            ,'SKY Sport 4'
            ,'SKY Sport 5'
            ,'SKY Sport 6'
            ,'SKY Sport 7'
            ,'SKY Sport 8'
            ,'SKY Sport 9'
            ]
    ## Get channel IDs
    channelURL = "https://static.sky.co.nz/sky/json/channels.prod.json"
    channelJS = requests.get(channelURL).json()
    channelIDdict = {
                        int(x['number']):x['name']
                        for x in channelJS
                        if x['name'] in channels
                    }
    channelIDs = list(channelIDdict.keys())
    ## Get programming
    url = f"https://web-epg.sky.co.nz/prod/epgs/v1?start={today9am}&end={tomorrow859am}&limit=20000"
    req = requests.get(url)
    relevantProgs = [
            x
            for x in req.json()['events']
            if x['channelNumber'] in channelIDs
            ]
    progs = []
    for rp in relevantProgs:
        tba = {}
        ## Start & End
        startUTC = datetime.utcfromtimestamp(int(rp['start'])/1000)
        endUTC = datetime.utcfromtimestamp(int(rp['end'])/1000)
        tba['StartLocal'] = utc_tz.localize(
                                    startUTC
                                        ).astimezone(
                                                nz_tz
                                                    ).replace(tzinfo=None)
        tba['StartUTC'] = startUTC
        tba['EndLocal'] = utc_tz.localize(
                                    endUTC
                                        ).astimezone(
                                                nz_tz
                                                    ).replace(tzinfo=None)
        tba['EndUTC'] = endUTC
        ## ProgrammeName
        tba['ProgrammeName'] = rp['title']
        ## Description
        tba['Description'] = rp['synopsis']
        ## Channel
        tba['Channel'] = channelIDdict[rp['channelNumber']]

        progs.append(tba)
        
    DF = pd.DataFrame(progs).sort_values('StartUTC').reset_index(drop=True)

    columnDict = {
        "StartLocal" : 'DateTime',
        "EndLocal" : 'DateTime',
        "StartUTC" : 'DateTime',
        "EndUTC" : 'DateTime',
        "Channel" : 'str',
        "ProgrammeName" : 'str',
        'Description' : 'str'
        }
    server = "nonDashboard"
    database = "WebScraping"
    sqlTableName = "NewZealandTVGuide"
    primaryKeyColName = "RowID"
        
    insertQ = create_insert_query(DF,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

    removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

    run_sql_commmand(removeDuplicatesQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_nz()

    logging.info("scraping done")

    return func.HttpResponse(f"Done")
