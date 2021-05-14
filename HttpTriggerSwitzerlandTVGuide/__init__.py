import logging
import requests
import pandas as pd
from datetime import datetime
import pytz
import azure.functions as func
from MyFunctions import (
    create_insert_query,
    create_removeDuplicates_query,
    run_sql_commmand
)

def scrape_switzerland():
    swTZ = pytz.timezone("Europe/Zurich")

    ## Get channel IDs
    r0 = requests.get(
            url="https://obo-prod.oesp.upctv.ch/oesp/v4/CH/eng/web/channels?byLocationId=100&includeInvisible=true&personalised=false&sort=channelNumber&withStationResolutions=SD%2CHD"
        )
    channelIDLookup = {
                ch['title'] : ch['id'].replace(":100-",":")
                for ch in r0.json()['channels']
            }
    channelIDLookupREV = {
                x:y
                for y,x in channelIDLookup.items()
            }

    channelsOfInterest = [
            "MySports One",
            "MySports One F"
            ]

    channelIDs = [channelIDLookup[x]
                    for x in channelsOfInterest]

    dateDT = datetime.now()

    dateStr = dateDT.strftime("%Y%m%d")

    dfList = []
    for i in range(1,5):
        r = requests.get(
                url=f"https://obo-prod.oesp.upctv.ch/oesp/v4/CH/eng/web/programschedules/{dateStr}/{i}"
            )

        js = r.json()
        for entrySubsection in js['entries']:
            if entrySubsection['o'] in channelIDs:
                df0 = pd.DataFrame(entrySubsection['l'])
                df0['o'] = entrySubsection['o']
                dfList.append(df0)
    
    ## Concat all DFs
    DF_ = pd.concat(dfList,sort=False,ignore_index=True)
    # DF0 = DF_.drop_duplicates()
    DF0 = DF_.copy()
    ## Create df to upload to SQL
    DF = pd.DataFrame()
    DF['Channel'] = DF0.o.map(channelIDLookupREV)
    DF['ProgrammeName'] = DF0.t
    DF['StartLocal'] = DF0.s.apply(lambda x: datetime.fromtimestamp(x/1000,swTZ))
    DF['StartUTC'] = DF0.s.apply(lambda x: datetime.utcfromtimestamp(x/1000))
    DF['EndLocal'] = DF0.e.apply(lambda x: datetime.fromtimestamp(x/1000,swTZ))
    DF['EndUTC'] = DF0.e.apply(lambda x: datetime.utcfromtimestamp(x/1000))


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
    sqlTableName = "SwitzerlandTVGuide"
    primaryKeyColName = "RowID"
        
    insertQ = create_insert_query(DF,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

    removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

    run_sql_commmand(removeDuplicatesQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_switzerland()

    return func.HttpResponse("Done")
