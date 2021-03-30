import logging
import requests
from bs4 import BeautifulSoup as BS
import pandas as pd
import pytz
from datetime import datetime, timedelta, time
import azure.functions as func
from MyFunctions import (
    create_insert_query,
    run_sql_commmand,
    create_removeDuplicates_query
)

def idStartswithHourNotHourbanner(tag):
    if tag.has_attr('id') == False:
        return False
    elif tag.attrs['id'] == "hourbanner":
        return False
    elif tag.attrs['id'].startswith("hour"):
        return True
    else:
        return False
    
def ampm(d):
    if d.hour <= 11:
        return "am"
    else:
        return "pm"

def scrape_netherlands():
    utc_tz = pytz.timezone("utc")
    nl_tz = pytz.timezone("Europe/Amsterdam")

    ## Get tomorrow's date in right format (for some reason today's doesn't work)
    dateToUse = datetime.now() + timedelta(days=1)
    dateString = dateToUse.strftime("%d-%m-%Y")

    ## Dict of TV channels
    tvChannels = {
            "ziggosportracing" : "Ziggo Sport Racing"
            }

    UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36"

    ## Empty dict for dfs
    dfs = {}

    for code,chan in tvChannels.items():
        ## Make request and get soup
        req = requests.get(
                f"https://www.tvgids.nl/gids/{dateString}/{code}",
                headers={'user-agent' : UA}
            )
        soup = BS(
                req.text,
                "html.parser"
            )
        ## Get block containing relevant information
        infoBlock = soup.find(
                        "div",
                        attrs={'class' : "guide__guide-container"})
        ## Get all the programme tags
        progTags = [x
                    for x in infoBlock.find_all('a')
                    if x.attrs['class'] == ['no-link', 'program', 'program--guide']]
        progs = []
        for pt in progTags:
            try:
                tba = {}
                ## Channel
                tba['Channel'] = chan
                ## Start time
                tba['StartTime'] = datetime.strptime(
                                    pt.find(
                                    'div',
                                    attrs={
                                        'class':'program__starttime'
                                            }).text.strip(),
                                    "%H:%M").time()
                ## Programme Name
                tba['ProgrammeName'] = pt.find(
                                        'h3',
                                        attrs={
                                            'class':'program__title'
                                                }).text.strip()
                ## Description
                tba['Description'] = pt.find(
                                        'p',
                                        attrs={
                                            'class':'program__text'
                                                }).text.strip()
                progs.append(tba)
            except AttributeError:
                pass
        
        ## Some progs are from the day before/after `dateString` so
        ##    we need to work out which ones those are
        startTimes = [x['StartTime'] for x in progs]
        AMsPMs = [ampm(x) for x in startTimes]
        daysDiff = []
        for i,ap in enumerate(AMsPMs):
            if i == 0:
                if ap == "am":
                    daysDiff.append(0)
                elif ap == "pm":
                    daysDiff.append(-1)
            else:
                if ap == "am":
                    if AMsPMs[i-1] == "am":
                        ## AM->AM, no day change, so same as last prog
                        daysDiff.append(daysDiff[i-1])
                    elif AMsPMs[i-1] == "pm":
                        ## PM->AM, next day, so plus one from last prog
                        daysDiff.append(daysDiff[i-1]+1)
                elif ap == "pm":
                    if AMsPMs[i-1] == "am":
                        ## AM->PM, no day change, so same as last prog
                        daysDiff.append(daysDiff[i-1])
                    elif AMsPMs[i-1] == "pm":
                        ## PM->PM, no day change, so same as last prog
                        daysDiff.append(daysDiff[i-1])
        for i,dicto in enumerate(progs):
            ## Set local time
            dicto['StartLocal'] = datetime.combine(
                                            date=dateToUse+timedelta(days=daysDiff[i]),
                                            time=dicto['StartTime']
                                        )
            ## Set UTC time
            dicto['StartUTC'] = nl_tz.localize(
                                    dicto['StartLocal']
                                        ).astimezone(
                                                utc_tz
                                                    ).replace(tzinfo=None)
        ## Create df from list
        df = pd.DataFrame(progs)
        del df['StartTime']
        ## Add EndLocal and EndUTC columns
        endLocalEnd = datetime.combine(
                            date=dateToUse+timedelta(days=1),
                            time=time(hour=6))
        df['EndLocal'] = df['StartLocal'].to_list()[1:] + [endLocalEnd]
        endUTCEnd = nl_tz.localize(
                                endLocalEnd
                                    ).astimezone(
                                            utc_tz
                                                ).replace(tzinfo=None)
        df['EndUTC'] = df['StartUTC'].to_list()[1:] + [endUTCEnd]
        ## Only the rows of progs starting on `dateToUse` will be uploaded,
        ##    so find those rows
        todayDD = [i for i,x in enumerate(daysDiff) if x == 0]
        minIndex = min(todayDD)
        maxIndex = max(todayDD)
        toSQLdf = df[(df.index >= minIndex) & (df.index <= maxIndex)].reset_index(drop=True)
        ## Add to dict
        dfs[code] = toSQLdf


    DF = pd.concat(dfs.values(),ignore_index=True)

    columnDict = {
        "StartLocal" : 'DateTime',
        "EndLocal" : 'DateTime',
        "StartUTC" : 'DateTime',
        "EndUTC" : 'DateTime',
        "Channel" : 'str',
        "ProgrammeName" : 'str',
        "Description" : 'str',
        }
    server = "nonDashboard"
    database = "WebScraping"
    sqlTableName = "NetherlandsTVGuide"
    primaryKeyColName = "RowID"
        
    insertQ = create_insert_query(DF,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

    removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

    run_sql_commmand(removeDuplicatesQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_netherlands()

    logging.info("scraping done")

    return func.HttpResponse(f"Done")
