import logging
import azure.functions as func
from datetime import datetime
from HttpTriggerUFCScheduleTSN import get_soup
from MyFunctions import (
    create_insert_query,
    run_sql_commmand
)
import pandas as pd



def get_all_rankings(BASE_URL,BASE_URL_UK,COL_HEADS):
    date_recorded = datetime.now()
    logging.info("{}: Collecting UFC rankings".format(date_recorded.strftime("%Y-%m-%d %H:%M")))
    page_soup = get_soup(BASE_URL)
    rankings = []
    
    # Get soups for ranking table for each weight division
    weight_categories = page_soup.findAll("div", "view-grouping")
    if len(weight_categories) == 0:
        logging.info("Looks like you've been redirected to the UK website!")
        
        # Go to the UK site for the rankings 
        page_soup = get_soup(BASE_URL_UK)
        weight_categories = page_soup.findAll("div", "ranking-list tall")
        
        for wc in weight_categories:
            rankings += get_rankings_for_category_uk(wc)
    
    else:
        # Get the rankings for each weight category
        for wc in weight_categories:
            rankings += get_rankings_for_category(wc)
    
    # Insert to SQL
    logging.info("Inserting rankings to SQL...")
    DF = pd.DataFrame(rankings, columns=COL_HEADS)
    DF["Recorded"] = date_recorded
    
    columnDict = {
        "Weight_Class" : 'str',
        "Ranking" : 'str',
        "Athlete" : 'str',
        "Recorded" : 'DateTime'
        }
    server = "nonDashboard"
    database = "UFC"
    sqlTableName = "Rankings"
    primaryKeyColName = "ID"
        
    insertQ = create_insert_query(DF,columnDict,sqlTableName)

    run_sql_commmand(insertQ,server,database)
    
def get_rankings_for_category(weight_category_soup):
    rankings = []
    # Weight category
    category = weight_category_soup.find("div", "view-grouping-header").text.replace("Top Rank", "").strip()
    logging.info("Getting rankings for {}...".format(category))
    p4p = category.lower() == "pound-for-pound"
    
    # Rankings
    try:
        champion = weight_category_soup.find("div", "view-grouping-content").find("div", "rankings--athlete--champion").a.text
        if p4p:
            rankings.append([category, 1, champion])
        else: 
            rankings.append([category, 0, champion])
    except AttributeError:
        pass
    rows = weight_category_soup.find("tbody").findAll("tr")
    for r in rows:
        rank = r.findAll("td")[0].text.strip()
        athlete = r.findAll("td")[1].a.text.strip()
        rankings.append([category, rank, athlete])
    return rankings
    
def get_rankings_for_category_uk(weight_category_soup):
    rankings = []
    # Weight category
    category = weight_category_soup.find("div", "weight-class-name").text.strip()
    logging.info("Getting rankings for {}...".format(category))
    p4p = category.lower() == "pound-for-pound"
    
    # Rankings
    fighters = [" ".join(el.a.string.split()) for el in weight_category_soup.findAll("td", "name-column")]
    if len(fighters) > 0:
        if not p4p:
            champion = weight_category_soup.find("div", "rankings-champions").a.text.strip()
            rankings.append([category, 0, champion])    
    else:
        logging.info("No fighters found for {} category".format(category))
        
    rankings += [[category, i + 1, el] for i, el in enumerate(fighters)]
    return rankings

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')


    COL_HEADS = ["Weight_Class", "Ranking", "Athlete"] 

    BASE_URL = "https://ufc.com/rankings/"
    BASE_URL_UK = "http://uk.ufc.com/rankings/"

    get_all_rankings(BASE_URL,BASE_URL_UK,COL_HEADS)

    return func.HttpResponse("Done")
