import logging
import requests
import pandas as pd
import azure.functions as func
from datetime import datetime, timedelta
from MyFunctions import (
    time2secs,
    create_insert_query,
    run_sql_commmand,
    create_removeDuplicates_query
)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')


    ## Get today's date in the right format
    todaysDate = datetime.strftime(
        datetime.now()
        ,"%Y%m%d"
    )

    channels = {
                "EPH1" : ("Fox Sports","Philippines"),
                "F2E1" : ("Fox Sports 2","Philippines"),
                "FM31" : ("Fox Sports 3","Philippines")
                }

    dfs = {}

    for channelCode,(channelName,country) in channels.items():
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
        ## Add to dict
        dfs[channelCode] = channelDF
        logging.info(f"channelName: {channelName}")
        logging.info(f"country: {country}")
        logging.info(f"rows: {len(channelDF)}")

    ## Concat dfs
    df = pd.concat(dfs.values(),ignore_index=True)
    logging.info(f"Total rows: {len(df)}")
    ## Compare `date` and `start_time` to make LocalStart
    df['LocalStart'] = [datetime.combine(
                            date=datetime.strptime(d,"%m-%d-%y").date(),
                            time=datetime.strptime(s,"%H:%M:%S").time()
                                    )
                        for d,s in zip(df.date,df.start_time)]
    ## Use `duration` to make LocalEnd
    df['LocalEnd'] = [ls + timedelta(seconds=time2secs(datetime.strptime(d,"%H:%M:%S").time()))
                    for ls,d in zip(df.LocalStart,df.duration)]
    ## Use `LocalStart` and `LocalEnd` to make UTCStart and UTCEnd (8 hour time difference)
    df['UTCStart'] = [ls - timedelta(hours=8)
                    for ls in df.LocalStart]
    df['UTCEnd'] = [le - timedelta(hours=8)
                    for le in df.LocalEnd]

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
    server = "non-dashboard"
    database = "WebScraping"
    sqlTableName = "FoxSports"
    primaryKeyColName = "RowID"
        
    insertQ = create_insert_query(df,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

    removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

    run_sql_commmand(removeDuplicatesQ,server,database)


    return func.HttpResponse(f"Done")