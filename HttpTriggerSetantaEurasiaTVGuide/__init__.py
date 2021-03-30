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

def scrape_setantaeurasia():    
    utc_tz = pytz.timezone("utc")
    ## TV Guide is in UTC+2 (Sofia is used to represent that)
    eet_tz = pytz.timezone("Europe/Sofia")

    tomorrowDT = datetime.now() + timedelta(days=1)
    tomorrowString = datetime.strftime(tomorrowDT,"%a-%b-%d").lower()

    url = "https://www.setantaeurasia.com/en/tv-listings/"
    req = requests.get(url)
    soup = BS(req.text,'html.parser')

    ccs = [
        ("setantasports1","Setanta Sports 1"),
        ("setantasports2","Setanta Sports 2")
        ]
    dfs = {}

    for code,clean in ccs:
        ## Get channel's panel
        panel = soup.find('div',id=code)
        ## Get tomorrow's tab
        tt = panel.find('div',id=f"tab-{tomorrowString}")
        ## Get all the progs
        progList = tt.find_all('li',attrs={'class':'event-detail-list__item'})
        progs = []
        for p in progList:
            tba = {}
            ## Channel
            tba['Channel'] = clean
            ## Start
            startStr = p.find('div',attrs={'class':'event-time'})['datetime']
            startDT = datetime.strptime(startStr,"%Y-%m-%dT%H:%M")
            tba['StartUTC'] = eet_tz.localize(
                                    startDT
                                        ).astimezone(
                                                utc_tz
                                                    ).replace(tzinfo=None)
            ## ProgrammeName
            pnList = []
            leagueNameElement = p.find('h3',attrs={'class':'event-league-name'})
            if leagueNameElement is not None:
                leagueName = leagueNameElement.text.strip()
                if len(leagueName) > 0:
                    pnList.append(leagueName)
            eventNameElement = p.find('h4',attrs={'class':'event-name'})
            if eventNameElement is not None:
                eventName = eventNameElement.text.strip()
                if len(eventName) > 0:
                    pnList.append(eventName)
            tba['ProgrammeName'] = " - ".join(pnList)
            ## Description
            tba['Description'] = p.find('p',attrs={'class':'event-description'}).text.strip()
            
            progs.append(tba)
            
        df = pd.DataFrame(progs)
        
        endDate = datetime.combine(
                date=tomorrowDT + timedelta(days=1),
                time=time(hour=0)
            )
        endUTCEnd = eet_tz.localize(
                                    endDate
                                        ).astimezone(
                                                utc_tz
                                                    ).replace(tzinfo=None)
        df['EndUTC'] = df['StartUTC'].to_list()[1:] + [endUTCEnd]
        
        dfs[code] = df
        
    DF = pd.concat(dfs.values(),ignore_index=True)

    columnDict = {
        "StartUTC" : 'DateTime',
        "EndUTC" : 'DateTime',
        "Channel" : 'str',
        "ProgrammeName" : 'str',
        'Description' : 'str'
        }
    server = "nonDashboard"
    database = "WebScraping"
    sqlTableName = "SetantaEurasiaTVGuide"
    primaryKeyColName = "RowID"
        
    insertQ = create_insert_query(DF,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

    removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

    run_sql_commmand(removeDuplicatesQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_setantaeurasia()

    logging.info("scraping done")

    return func.HttpResponse(f"Done")
