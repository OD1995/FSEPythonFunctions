import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup as BS
from MyFunctions import (
    create_insert_query,
    run_sql_commmand
)
from datetime import datetime
import azure.functions as func

def get_soup(URL):
    r = requests.get(URL)
    return BS(r.text,'html.parser')

def get_schedule_url(BASE_URL):
    logging.info("Navigating to schedule...")
    # Go to TSN UFC home page
    home_page_soup = get_soup(BASE_URL)
    
    # Find TSN schedule element 
    nav_elements = home_page_soup.find("div", {"id" : "content-container"}).find("ul", "site-nav").findAll("li")
    schedule_url = BASE_URL + [li.a["href"] for li in nav_elements if li.text.strip().lower() == "ufc on tsn"][0]
    return schedule_url

def get_tsn_schedule(BASE_URL,COL_HEADS):
    date_recorded = datetime.now()
    logging.info("{}: Collecting TSN UFC schedule".format(date_recorded.strftime("%Y-%m-%d %H:%M")))
    # Scrape the event names, dates and TSN feeds
    schedule_url = get_schedule_url(BASE_URL)
    schedule_page_soup = get_soup(schedule_url)
    
    
    # Extract rows from schedule table
    schedule_data = []
    rows = schedule_page_soup.find("tbody").findAll("tr")
    for r in rows:
        schedule_data.append([td.text.strip() for td in r.findAll("td")] + [date_recorded])
        
    # Put into dataframe to be inserted into SQL
    DF = pd.DataFrame(schedule_data, columns=COL_HEADS)
    
    columnDict = {
        "Broadcast_Date" : 'str',
        "Event" : 'str',
        "Broadcast_Time" : 'str',
        "Network" : 'str',
        "Recorded" : 'DateTime'
        }
    server = "nonDashboard"
    database = "UFC"
    sqlTableName = "TSNScheduleRaw"
    primaryKeyColName = "ID"
        
    insertQ = create_insert_query(DF,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    COL_HEADS = ["Broadcast_Date", "Event", "Broadcast_Time", "Network", "Recorded"] 
    BASE_URL = "https://www.tsn.ca/ufc"
    get_tsn_schedule(BASE_URL,COL_HEADS)

    return func.HttpResponse("Done")
