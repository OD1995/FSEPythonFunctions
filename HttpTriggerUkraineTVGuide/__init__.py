import logging
from numpy.lib.utils import info
import requests
from bs4 import BeautifulSoup as BS
import pandas as pd
from googletrans import Translator
import pytz
from datetime import datetime, timedelta, time
import azure.functions as func
from MyFunctions import (
    create_insert_query,
    run_sql_commmand,
    create_removeDuplicates_query
)

def scrape_ukraine():

    ### GETS YESTERDAY'S PROGRAMMING, NOT TODAY'S

    yesterdaysDate = (datetime.now() - timedelta(days=1)).date()
    utc_tz = pytz.timezone("utc")
    ukr_tz = pytz.timezone('Europe/Kiev')

    logging.info(f"yesterdaysDate: {yesterdaysDate}")

    days = [
        'monday',
        'tuesday',
        'wednesday',
        'thursday',
        'friday',
        'saturday',
        'sunday',
    ]

    translator = Translator()

    chans = [
        ('Setanta Sports Ukraine',1451)
    ]

    for channel_name, channelID in chans:
        prog_url = f'https://tv.meta.ua/{days[yesterdaysDate.weekday()]}/'
        logging.info(f"prog_url:`{prog_url}`")
        r = requests.get(
            url=prog_url,
            headers={
                'cookie' : f'_chnls={channelID}'
                    }
            )
        logging.info(f"channel_name: {channel_name}")
        row_list = []
        
        soup = BS(r.text,'html.parser')
        tableSoup = soup.find(
            'table',
            attrs={
                'class' : 'channel-inner-table'
            }
        )
        for tr in tableSoup.find_all('tr'):
            ## Get all the divs
            divs = tr.find_all(
                'div',
                attrs={
                    'style' : 'clear:both'
                }
            )
            for div in divs:
                tba = {
                    'Channel' : channel_name
                }
                start_time_str = div.find(
                    'div',
                    attrs={
                        'class' : ['ptime_a','ptime']
                    }
                ).text
                start_time_dt = datetime.strptime(
                    start_time_str,
                    "%H:%M"
                )
                ## If before 6am, it's the next day's programming
                if start_time_dt.hour < 6:
                    xt = 1
                else:
                    xt = 0
                tba['StartLocal'] = datetime.combine(
                    date=yesterdaysDate + timedelta(days=xt),
                    time=start_time_dt.time()
                )
                tba['StartUTC'] = ukr_tz.localize(
                    tba['StartLocal']
                ).astimezone(
                    utc_tz
                ).replace(tzinfo=None)
                
                russian_progname = div.find(
                    'div',
                    attrs={
                        'style' : 'display:table; _height:0; '
                    }
                ).text
                tba['ProgrammeName'] = translator.translate(
                    text=russian_progname,
                    src='ru',
                    dest='en'
                ).text
                
                row_list.append(tba)
                
        DF = pd.DataFrame(row_list).sort_values(
            'StartLocal'
        ).reset_index(drop=True)
        
        last_dt_local = datetime.combine(
            date=yesterdaysDate+timedelta(days=1),
            time=time(hour=6)
        )
        last_dt_utc = ukr_tz.localize(
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
        sqlTableName = "UkraineTVGuide"
        primaryKeyColName = "RowID"
            
        insertQ = create_insert_query(DF,columnDict,sqlTableName)
        logging.info(f"insertQ: {insertQ}")

        run_sql_commmand(insertQ,server,database)

        removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

        run_sql_commmand(removeDuplicatesQ,server,database)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_ukraine()

    logging.info("scraping done")

    return func.HttpResponse(f"Done")
