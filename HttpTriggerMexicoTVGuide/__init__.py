import logging

import azure.functions as func

from datetime import datetime, timedelta, time, date
from azure.functions._thirdparty.werkzeug import datastructures
import requests
from bs4 import BeautifulSoup as BS
import pytz
from tzlocal import get_localzone
import pandas as pd
from MyFunctions import (
    create_insert_query,
    run_sql_commmand,
    create_removeDuplicates_query
)

def scrape_mexico(
    dateStr=None
):    
    ## Get the system's timezone
    system_tz = get_localzone()
    utc_tz = pytz.timezone("utc")
    mx_tz = pytz.timezone("America/Mexico_City")
    if dateStr is None:
        dateDT = datetime.now()
        dateStr = dateDT.strftime("%Y-%m-%d")
    else:
        dateDT = datetime.strptime(dateStr,"%Y-%m-%d")
    logging.info(f"dateStr: {dateStr}")
    ccs = [
        ("claro_sports","Claro Sports")
        ]

    C = [
        "tbl_EPG_row",
        "tbl_EPG_rowAlternate"
        ]
    progs = []
    for code,clean in ccs:
        
        url = f"https://www.gatotv.com/canal/{code}/{dateStr}"
        logging.info(f"url: {url}")
        req = requests.get(url)
        soup = BS(req.text,'html.parser')
        
        ## Get EPG table
        epgTable = soup.find('table',attrs={'class':'tbl_EPG'})
        ## Get programme rows
        progRows0 = [x
                    for x in epgTable.find_all('tr')
                    if 'class' in x.attrs
                    ]
        progRows = [x
                    for x in progRows0
                    if x.attrs['class'][0] in C]
        for pr in progRows:
            tba = {}
            ## Start & End
            startDT,endDT = [datetime.combine(
                                date=dateDT,
                                time=datetime.strptime(x.text.strip(),"%H:%M").time())
                            for x in pr.find_all('div')[:2]]
            
            tba['StartLocal'] = system_tz.localize(
                                    startDT
                                        ).astimezone(
                                                mx_tz
                                                    ).replace(tzinfo=None)
            tba['StartUTC'] = system_tz.localize(
                                    startDT
                                        ).astimezone(
                                                utc_tz
                                                    ).replace(tzinfo=None)
            tba['EndLocal'] = system_tz.localize(
                                    endDT
                                        ).astimezone(
                                                mx_tz
                                                    ).replace(tzinfo=None)
            tba['EndUTC'] = system_tz.localize(
                                    endDT
                                        ).astimezone(
                                                utc_tz
                                                    ).replace(tzinfo=None)
            ## ProgrammeName
            tba['ProgrammeName'] = pr.find('div',
                                    attrs={
                                    'class':"div_program_title_on_channel"
                                                }).text.strip()
            ## Channel
            tba['Channel'] = clean
        
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
    sqlTableName = "MexicoTVGuideFromAzure"
    primaryKeyColName = "RowID"
        
    insertQ = create_insert_query(DF,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

    removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

    run_sql_commmand(removeDuplicatesQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    dateStr = req.params.get("dateStr")

    scrape_mexico(dateStr)

    logging.info("scraping done")

    return func.HttpResponse(f"Done")
