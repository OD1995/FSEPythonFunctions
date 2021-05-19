import datetime
from HttpTriggerUFCScheduleTSN import get_tsn_schedule
import logging

import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    COL_HEADS = ["Broadcast_Date", "Event", "Broadcast_Time", "Network", "Recorded"] 
    BASE_URL = "https://www.tsn.ca/ufc"
    get_tsn_schedule(BASE_URL,COL_HEADS)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
