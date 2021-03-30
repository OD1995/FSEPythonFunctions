import logging
import requests
import pandas as pd
import azure.functions as func
import pytz
from datetime import datetime, timedelta
from MyFunctions import (
    time2secs,
    create_insert_query,
    run_sql_commmand,
    create_removeDuplicates_query
)

def scrape_FoxSports():
    ## Get today's date in the right format
    todaysDate = datetime.strftime(
        datetime.now()
        ,"%Y%m%d"
    )

    channels = {
        ("Fox Sports","Philippines") : "EPH1",
        ("Fox Sports 2","Philippines") : "F2E1",
        ("Fox Sports 3","Philippines") : "FM31",
        ("Fox Sports","Malaysia") : "EML1",
        ("Fox Sports 2","Malaysia") : "F2M1",
        ("Fox Sports 3","Malaysia") : "FM31",
        ("Fox Sports","Singapore") : "ESG1",
        ("Fox Sports 2","Singapore") : "F2S1",
        ("Fox Sports 3","Singapore") : "FM31",
        ("Star Sports","China") : "SCN1",
        ("Star Sports2","China") : "ECN1",
    }

    timezones = {
        "Philippines" : "Asia/Manila",
        "Malaysia" : "Asia/Kuala_Lumpur",
        "Singapore" : "Asia/Singapore",
        "China" : "Asia/Shanghai",
    }
    utc_tz = pytz.timezone("utc")

    dfs = {}

    for channelCode,(channelName,country) in channels.items():
        loc_tz = pytz.timezone(timezones[country])
        reqURL = "https://tv.foxsportsasia.com/getEPG.php"
        reqParams = {
                        "lang" : "en",
                        "channelCode" : channelCode,
                        "date" : todaysDate
                    }
        ## Make request and get response
        r = requests.get(reqURL,params=reqParams)
        js = r.json()[channelCode]
        ## Create pandas df from JSON
        channelDF = pd.DataFrame(js)
        ## Add channel name and coutnry as columns
        channelDF['ChannelName'] = channelName
        channelDF['Country'] = country

        ## Compare `date` and `start_time` to make LocalStart
        channelDF['LocalStart'] = [datetime.combine(
                                date=datetime.strptime(d,"%m-%d-%y").date(),
                                time=datetime.strptime(s,"%H:%M:%S").time()
                                        )
                            for d,s in zip(channelDF.date,channelDF.start_time)]
        ## Use `duration` to make LocalEnd
        channelDF['LocalEnd'] = [
            ls + timedelta(seconds=time2secs(datetime.strptime(d,"%H:%M:%S").time()))
            for ls,d in zip(
                channelDF.LocalStart,
                channelDF.duration
                )
        ]
        ## Use `LocalStart` and `LocalEnd` to make UTCStart and UTCEnd
        channelDF['UTCStart'] = [
                loc_tz.localize(ls).astimezone(utc_tz).replace(tzinfo=None)
                for ls in channelDF.LocalStart
        ]
        channelDF['UTCEnd'] = [
                loc_tz.localize(le).astimezone(utc_tz).replace(tzinfo=None)
                for le in channelDF.LocalEnd
        ]
        ## Add to dict
        dfs[channelCode] = channelDF
        logging.info(f"channelName: {channelName}")
        logging.info(f"country: {country}")
        logging.info(f"rows: {len(channelDF)}")

    ## Concat dfs
    df = pd.concat(dfs.values(),ignore_index=True)
    logging.info(f"Total rows: {len(df)}")
    ## Remove the unused columns
    removeMes = [
                    'date',
                    'start_time',
                    'duration',
                    'dow'
                ]
    for rem in removeMes:
        del df[rem]
        
    columnDict = {
            'channel_code' : 'str',
            'sub_genre' : 'str',
            'genre' : 'str',
            'live' : 'str',
            'programme' : 'str',
            'matchup' : 'str',
            'ChannelName' : 'str',
            'Country' : 'str',
            'LocalStart' : 'DateTime',
            'LocalEnd' : 'DateTime',
            'UTCStart' : 'DateTime',
            'UTCEnd' : 'DateTime'
            }
    server = "nonDashboard"
    database = "WebScraping"
    sqlTableName = "FoxSports"
    primaryKeyColName = "RowID"
        
    insertQ = create_insert_query(df,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

    removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

    run_sql_commmand(removeDuplicatesQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_FoxSports()

    logging.info("scraping done")

    return func.HttpResponse(f"Done")