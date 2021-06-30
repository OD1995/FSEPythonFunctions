import logging
import requests
from bs4 import BeautifulSoup as BS
import pandas as pd
from datetime import datetime, timedelta, time
import pytz
import azure.functions as func
from MyFunctions import (
    create_insert_query,
    run_sql_commmand,
    create_removeDuplicates_query
)

def scrape_southafrica():
    todayDate = datetime.now().date()    
    utc_tz = pytz.timezone("utc")
    sa_tz = pytz.timezone("Africa/Johannesburg")


    chans = [
        ('SABC Sport','SABC%20Sport')
            ]

    for channel_name,channel_code in chans:

        r = requests.get(f'https://tvguide.etv.co.za/guide/{channel_code}')
        
        soup = BS(r.text,'html.parser')
        
        row_list = []
        
        #progBoxes = todaySection.find('tbody').find_all('tr')
        #print(len(progBoxes))
        for progBoxTable in soup.find_all('table')[:3]:
            for pb in progBoxTable.find_all('tr'):
                tba = {
                    'Channel' : channel_name
                }
                tds = pb.find_all('td')
                ## Time
                tba['StartLocal'] = datetime.combine(
                    date=todayDate,
                    time=datetime.strptime(
                        tds[0].text,
                        "%I:%M %p"
                    ).time()
                )
                tba['StartUTC'] = sa_tz.localize(
                    tba['StartLocal']
                ).astimezone(
                    utc_tz
                ).replace(tzinfo=None)
                ## Programme
                if pb.b.text != 'Currently playing':
                    tba['ProgrammeName'] = pb.b.text
                else:
                    tba['ProgrammeName'] = pb.h3.text
                
                row_list.append(tba)
        
        
        DF = pd.DataFrame(row_list).sort_values(
            'StartLocal'
        ).reset_index(drop=True)
        
        last_dt_local = datetime.combine(
            date=todayDate+timedelta(days=1),
            time=time(hour=0)
        )
        last_dt_utc = sa_tz.localize(
            last_dt_local
        ).astimezone(
            utc_tz
        ).replace(tzinfo=None)
        
        DF['EndLocal'] = DF.StartLocal.to_list()[:-1] + [last_dt_local]
        DF['EndUTC'] = DF.StartUTC.to_list()[:-1] + [last_dt_utc]

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
        sqlTableName = "SouthAfricaTVGuide"
        primaryKeyColName = "RowID"
            
        insertQ = create_insert_query(DF,columnDict,sqlTableName)
        logging.info(f"insertQ: {insertQ}")

        run_sql_commmand(insertQ,server,database)

        removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

        run_sql_commmand(removeDuplicatesQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_southafrica()

    logging.info("scraping done")

    return func.HttpResponse(f"Done")
