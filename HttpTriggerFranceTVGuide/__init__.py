import logging
import requests
from bs4 import BeautifulSoup as BS
import pandas as pd
from datetime import datetime, timedelta, time
import azure.functions as func
from MyFunctions import (
    create_insert_query,
    run_sql_commmand,
    create_removeDuplicates_query
)

def scrape_france():

    dt = datetime.now().date().strftime("%Y-%m-%d")

    chans = ['eurosport-1-5',
            'eurosport-2-63',
            'canalplus-decale-36',
            'c8-4',
            'cstar-28',
            'canalplus-2']

    dfs = {}

    for chan in chans:
            
        url = f"https://www.programme-tv.net/programme/chaine/{dt}/programme-{chan}.html"
        
        pyDt = datetime.strptime(dt,"%Y-%m-%d").date()
        
        req = requests.get(url)
        
        soup = BS(req.text, 'html.parser')
        
        channelName = soup.find('span',attrs={'class' : 'gridChannel-title'}).text
        
        progs = soup.find_all('div',attrs={'class':'singleBroadcastCard'})
        
        starts_ = [None if x.find('div',attrs={'class' : 'singleBroadcastCard-hour'}) is None \
                else x.find('div',attrs={'class' : 'singleBroadcastCard-hour'}).text.replace("\n","").strip() \
                    for x in progs]
        starts = [datetime.combine(pyDt,time(hour=int(x.split("h")[0]),minute=int(x.split("h")[1])))
                    for x in starts_]
        
        titles = [None if x.find('a',attrs={'class' : 'singleBroadcastCard-title'}) is None \
                else x.find('a',attrs={'class' : 'singleBroadcastCard-title'}).text.replace("\n","").strip() \
                    for x in progs]
        
        subtitles_ = [None if x.find('div',attrs={'class' : 'singleBroadcastCard-subtitle'}) is None \
                    else x.find('div',attrs={'class' : 'singleBroadcastCard-subtitle'}).text.replace("\n","").strip() \
                    for x in progs]
        subtitles = [None if len(x) == 0 else x for x in subtitles_]
        
        genres_ = [None if x.find('div',attrs={'class' : 'singleBroadcastCard-genre'}) is None \
                else x.find('div',attrs={'class' : 'singleBroadcastCard-genre'}).text.replace("\n","").strip() \
                    for x in progs]
        genres = [None if len(x) == 0 else x for x in genres_]
        
        durations_ = [None if x.find('span',attrs={'class' : 'singleBroadcastCard-durationContent'}) is None \
                    else x.find('span',attrs={'class' : 'singleBroadcastCard-durationContent'}).text.replace("\n","").replace("min","").strip() \
                    for x in progs]
        durations = [timedelta(minutes=int(x)) if x.isdigit() \
                        else timedelta(hours=int(x.split("h")[0])) if x[-1] == "h" \
                        else timedelta(hours=int(x.split("h")[0]), minutes=int(x.split("h")[1]))
                        for x in durations_]
        
        ends = [x + y for x,y in zip(starts,durations)]
        
        
        df = pd.DataFrame({'Start' : starts,
                        'End' : ends,
                        'Title' : titles,
                        'Subtitle' : subtitles,
                        'Genre' : genres})
        df['Channel'] = channelName
        
        dfs[f"{channelName}-{dt}"] = df
        
        
    DF = pd.concat(dfs.values(),ignore_index=True,sort=False)

    columnDict = {
        "Start" : 'DateTime',
        "End" : 'DateTime',
        "Title" : 'str',
        "Subtitle" : 'str',
        "Genre" : 'str',
        "Channel" : 'str'
        }
    server = "nonDashboard"
    database = "WebScraping"
    sqlTableName = "FranceTVGuide"
    primaryKeyColName = "RowID"
        
    insertQ = create_insert_query(DF,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

    removeDuplicatesQ = create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName)

    run_sql_commmand(removeDuplicatesQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_france()

    logging.info("scraping done")

    return func.HttpResponse(f"Done")
