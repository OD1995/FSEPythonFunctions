import datetime
import logging
from HttpTriggerUFCRankings import get_all_rankings
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    COL_HEADS = ["Weight_Class", "Ranking", "Athlete"] 

    BASE_URL = "https://ufc.com/rankings/"
    BASE_URL_UK = "http://uk.ufc.com/rankings/"

    get_all_rankings(BASE_URL,BASE_URL_UK,COL_HEADS)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
