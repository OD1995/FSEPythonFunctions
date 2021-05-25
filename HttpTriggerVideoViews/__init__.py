from datetime import datetime
import logging
import requests
import json
from MyFunctions import (
    get_df_from_sqlQuery,
    run_sql_commmand
)
from bs4 import BeautifulSoup as BS
import azure.functions as func
import os
import re
from azure.storage.blob import BlockBlobService

def get_bbs():
    return BlockBlobService(
        connection_string=os.getenv("fsecustomvisionimagesCS")
    )

def save_text(
    html,
    rowID,
    _format_="txt"
):
    bbs = get_bbs()
    if _format_ == "txt":
        bbs.create_blob_from_text(
            container_name="test",
            blob_name=f"{rowID}.txt",
            text=html
        )
    elif _format_ == "js":
        bbs.create_blob_from_text(
            container_name="test",
            blob_name=f"{rowID} JS.json",
            text=str(html)
        )       

def insta_get_video_views(session,URL,mediaType,rowID,UA):
    H = {"user-agent" : UA}
    req = session.get(URL,headers=H)
    logging.info(req.request.headers)
    html = req.text
    save_text(
        html=html,
        rowID=rowID
    )
    soup = BS(html,'html.parser')
    script = [x
              for x in soup.find_all('script',{'type':'text/javascript'})
              if "video_view_count" in str(x)][0]
    if "window._sharedData" in str(script):
        logging.info("window._sharedData")
        dataStr = str(script).replace(
            '<script type="text/javascript">window._sharedData = ',
            ''
        ).replace(
            ';</script>',
            ''
        )
        data = json.loads(dataStr)
        shortcodeMedia = data['entry_data']['PostPage'][0]['graphql']['shortcode_media']
    elif "window.__additionalDataLoaded" in str(script):
        logging.info("window.__additionalDataLoaded")
        postBase = URL.replace(
            "https://www.instagram.com",
            ""
        )
        dataStr = str(script).replace(
            f"""<script type="text/javascript">window.__additionalDataLoaded('{postBase}',""",
            ""
        ).replace(
            ');</script>',
            ''
        )
        data = json.loads(dataStr)
        shortcodeMedia = data['graphql']['shortcode_media']
    save_text(
        html=data,
        rowID=rowID,
        _format_="js"
    )
    if mediaType != 'album':
        num = shortcodeMedia['video_view_count']
    else:
        num = 0
        for edge in shortcodeMedia['edge_sidecar_to_children']['edges']:
            if "video_view_count" in edge['node']:
                num += edge['node']["video_view_count"]
    
    return int(num)

def fb_get_video_views(session,URL,rowID,userAgent):
    H = {"user-agent" : userAgent}
    req = session.get(
        URL,
        headers=H
    )
    html = req.text
    save_text(
        html=html,
        rowID=rowID
    )
    soup = BS(html, "html.parser")
    split_one = str(soup).split("_1vx9")[-1]
    split_two = split_one.split(" Views")[0]
    try:
        num = int(split_two.replace('"><span>', '').replace(',', ''))
    except ValueError:
        num = "nope"
    return num

def insta_login(session,userAgent):
    """Have Y attempts at logging in"""
    Y = 3
    for i in range(Y):
        result = insta_login_inner(
            session=session,
            userAgent=userAgent
        )
        if result:
            break
    if not result:
        raise ValueError("unable to login to Instagram")

def insta_login_inner(session,userAgent):
    instaLink = 'https://www.instagram.com/accounts/login/'
    instaLoginURL = 'https://www.instagram.com/accounts/login/ajax/'
    instaUsername = "futuressport2"
    instaPassword = "Goldwing1!"
    session.headers= {"user-agent":userAgent}
    session.headers.update({"Referer":instaLink})
    time = int(datetime.now().timestamp())
    payload = {
        'username': instaUsername,
        'enc_password': f'#PWD_INSTAGRAM_BROWSER:0:{time}:{instaPassword}',
        'queryParams': {},
        'optIntoOneTap': 'false'
    }    
    instaCSRFReq = session.get(instaLink)
    csrf = re.findall(r"csrf_token\":\"(.*?)\"",instaCSRFReq.text)[0]
    instaLoginReq = session.post(
        instaLoginURL,
        data=payload,
        headers={
            "user-agent": userAgent,
            "x-requested-with": "XMLHttpRequest",
            "referer": "https://www.instagram.com/accounts/login/",
            "x-csrftoken":csrf
        }
    )
    instaLoginJS = instaLoginReq.json()
    logging.info(f"instaLoginReq: {instaLoginJS}")

    if "authenticated" in instaLoginJS:
        return instaLoginJS['authenticated']
    else:
        return False

def scrape_topX(X):

    if X is None:
        X = 100
    elif isinstance(X,str):
        X = str(X)
    
    ## Get top X rows
    Q = f"""
SELECT				TOP {X}
					O.RowID,
					O.Master_PostID,
					O.PlatformName,
					O.VideoViews,
                    O.DateScraped,
					O.ScrapeCount,
					M.Article_Timestamp,
					T.MediaType,
                    T.Link
FROM				ObservedSocialVideoViews O
LEFT JOIN			MASTER_ClientArticles M
	ON				M.Master_PostID=O.Master_PostID
LEFT JOIN			[Toolkit_SourceTable_CrowdTangle_Octagon] T
	ON				M.SourceTable_LocalID = T.ImportId
WHERE				O.VideoViews IS NULL
ORDER BY			M.Article_Timestamp ASC
    """
    initialTopXdf = get_df_from_sqlQuery(
        sqlQuery=Q,
        database="GlobalMultimedia"
    )
    ## Start session
    userAgent= "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
    
    with requests.Session() as s:
        ## Login to Instagram
        insta_login(
            session=s,
            userAgent=userAgent
        )
        ## Loop through the rows, scrape and update the table
        for i in initialTopXdf.index:
            logging.info(f"RowID: {initialTopXdf.loc[i,'RowID']}")
            scrapeSuccess = False
            if "www.facebook.com" in initialTopXdf.loc[i,"Link"]:
                videoViews = fb_get_video_views(
                    session=s,
                    URL=initialTopXdf.loc[i,"Link"],
                    rowID=initialTopXdf.loc[i,'RowID'],
                    userAgent=userAgent
                )
                scrapeSuccess = isinstance(videoViews,int)
            elif "www.instagram.com" in initialTopXdf.loc[i,"Link"]:
                videoViews = insta_get_video_views(
                    session=s,
                    URL=initialTopXdf.loc[i,"Link"],
                    mediaType=initialTopXdf.loc[i,"MediaType"],
                    rowID=initialTopXdf.loc[i,'RowID'],
                    UA=userAgent
                )
                scrapeSuccess = isinstance(videoViews,int)
            else:
                logging.info(f"Neither FB nor Instagram: {initialTopXdf.loc[i,'Link']}")

            if scrapeSuccess:
                dateScraped = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                ## Update the SQL table
                uC = f"""
    UPDATE ObservedSocialVideoViews
    SET VideoViews = {videoViews}, DateScraped = '{dateScraped}', ScrapeCount = 1
    WHERE Master_PostID = '{initialTopXdf.loc[i,"Master_PostID"]}'            
                """
                run_sql_commmand(
                    query=uC,
                    database="GlobalMultimedia",
                    server="nonDashboard"
                )
                logging.info("SUCCESS")

            else:
                logging.info("FAILURE")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_topX(
        X=req.params.get('X')
    )

    return func.HttpResponse("Done")
