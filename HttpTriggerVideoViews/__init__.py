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

def insta_get_video_views(URL,mediaType):
    UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36"
    H = {"user-agent" : UA}
    html = requests.get(URL,headers=H).content
    soup = BS(html,'html.parser')
    script = [x
              for x in soup.find_all('script',{'type':'text/javascript'})
              if "video_view_count" in str(x)][0]
    dataStr = str(script).replace(
                    '<script type="text/javascript">window._sharedData = ', ''
            ).replace(
                    ';</script>',''
            )
    data = json.loads(dataStr)
    shortcodeMedia = data['entry_data']['PostPage'][0]['graphql']['shortcode_media']
    if mediaType != 'album':
        num = shortcodeMedia['video_view_count']
    else:
        num = 0
        for edge in shortcodeMedia['edge_sidecar_to_children']['edges']:
            if "video_view_count" in edge['node']:
                num += edge['node']["video_view_count"]
    
    return int(num)

def fb_get_video_views(url):
    html = requests.get(url).content
    soup = BS(html, "html.parser")
    split_one = str(soup).split("_1vx9")[-1]
    split_two = split_one.split(" Views")[0]
    num = int(split_two.replace('"><span>', '').replace(',', ''))
    return num

def scrape_topX(X):

    if X is None:
        X = 100
    
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
    ## Loop through the rows, scrape and update the table
    for i in initialTopXdf.index:
        scrapeSuccess = False
        if "www.facebook.com" in initialTopXdf.loc[i,"Link"]:
            videoViews = fb_get_video_views(
                url=initialTopXdf.loc[i,"Link"]
            )
            scrapeSuccess = True
        elif "www.instagram.com" in initialTopXdf.loc[i,"Link"]:
            videoViews = insta_get_video_views(
                URL=initialTopXdf.loc[i,"Link"],
                mediaType=initialTopXdf.loc[i,"MediaType"]
            )
            scrapeSuccess = True
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
                database="GlobalMultimedia"
            )

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_topX(
        X=req.params.get('X')
    )

    return func.HttpResponse("Done")
