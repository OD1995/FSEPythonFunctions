import logging

import azure.functions as func


#import azure-storage-blob
#import urllib.request

# from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient
# import s3fs
# from io import BytesIO
# import tweepy
import pandas as pd
# import numpy as np
import datetime
# from dateutil.parser import parse
# import json
# import re
#from urllib.request import urlopen
# from bs4 import BeautifulSoup as bs
# import boto3


def scrape_CrowdTangle():

    #blob_service_client = BlobServiceClient(account_url=sas_url, credential=sas_token)
    sas_url='https://octagonsocialdata.blob.core.windows.net/?sv=2019-12-12&ss=b&srt=co&sp=rwlacx&se=2030-09-17T18:10:12Z&st=2020-09-17T10:10:12Z&spr=https&sig=zmJBn%2BDiUbaQozYvMkyj%2FCDUUI1PGo7%2BfBWTMnsvTRY%3D'
    sas_token =  '?sv=2019-12-12&ss=b&srt=co&sp=rwlacx&se=2030-09-17T18:10:12Z&st=2020-09-17T10:10:12Z&spr=https&sig=zmJBn%2BDiUbaQozYvMkyj%2FCDUUI1PGo7%2BfBWTMnsvTRY%3D'
    blob_service_client = BlobServiceClient(account_url=sas_url, credential=sas_token)

    fb_api_token = 'vFIzJxjrUp83wKc2ZK6RhfGy3Eu3Ud0OCJaDkDcc'
    insta_api_token = 'T8rE357isKx2WzigmPznBXqvC7rsZAfS4AyGvYsy'

    bucket = 'futures-fb-daily'

    cpm_value = 9
    list_of_terms = [""] #['pros', 'cisco']
    start_date_val = str(datetime.date.today() - datetime.timedelta(days=1))
    end_date_val = str(datetime.date.today())
    list_of_campaigns = [['futures_aus', 1422513],
                        ['futures_uk', 1423613],
                        ['futures_us', 1440032]]

    list_of_ig_campaigns = [['futures_ig_uk', 1462837 ],
                            ['futures_ig_aus', 1462861]]

    #exec('./crowdtangle_api.py')
    exec(open('/home/ubuntu/AWS-Lightsail/crowdtangle_api.py').read())
    d = Dashboard(api_url)

    # convert list to the string format needed for crowdtangle
    str_list = ''
    for i in list_of_terms:
        str_list += (i + ', ')
    terms = str_list[:-2]





    for campaign in list_of_campaigns:
        facebook_posts = d.get_posts(start_date_val, campaign[1], fb_api_token, terms, end_date_val)

        fb_posts = []


        for i in facebook_posts:
            try:
                message = i['message'].lower()
            except:
                message = ''
            platform = 'Facebook'
            author = i['account']['name']
            profile_img = i['account']['profileImage']
            try:
                user_name = i['account']['handle']
            except:
                user_name = ''
            reach = i['account']['subscriberCount']
            try:
                post_id = i['platformId'].split('_')[1] # first part is user_id, but don't need
            except:
                post_id = i['platformId'].split(':')[0] # sometimes it is this format?
            time_stamp = i['date']
            text = message
            rts_shares = i['statistics']['actual']['shareCount']
            faves_likes = i['statistics']['actual']['likeCount']
            comments = i['statistics']['actual']['commentCount']
            media_type = i['type']
            try:
                link = i['link']
            except:
                link = ''
            try:
                media_url = i['media'][0]['url']
            except:
                media_url = ''
            quot_text = ''
            quot_img = ''
            fb_posts.append([platform,
                            author,
                            user_name,
                            reach,
                            post_id,
                            time_stamp,
                            text,
                            rts_shares,
                            faves_likes,
                            comments,
                            media_type,
                            link,
                            media_url,
                            quot_text,
                            quot_img,
                            profile_img
                            ])

        facebook_df = pd.DataFrame(columns= ['platform_name',
                                            'author',
                                            'user_name',
                                            'reach',
                                            'post_id',
                                            'time_stamp',
                                            'text',
                                            'rts_shares',
                                            'faves_likes',
                                            'comments',
                                            'media_type',
                                            'link',
                                            'media_url',
                                            'quot_text',
                                            'quot_img',
                                            'profile_img'],
                                data = fb_posts)
        bytes_to_write = facebook_df.to_csv(None).encode()
    #    fs = s3fs.S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY)
    #    with fs.open('s3://futures-fb-daily/' + campaign[0] + '-' + str(datetime.date.today()) + '.csv', 'wb') as f:
    #        f.write(bytes_to_write)
        blob_client = blob_service_client.get_blob_client("philcontainertest", campaign[0] + "-" + str(datetime.date.today()) + "_accounts.csv")
        blob_client.upload_blob(bytes_to_write, blob_type="BlockBlob")

    # file_name = campaign[0] + '-' + str(datetime.date.today()) +  '.csv'
    # csv_buffer = BytesIO()
    # facebook_df.to_csv(csv_buffer).encode('utf-8')
    # s3_resource = boto3.resource('s3')
    # s3_resource.Object(bucket, file_name).put(Body=csv_buffer.get_value())
    #  facebook_df.to_csv(file_name,  encoding='utf-8')
    # print(file_name)
    # upload_to_aws('./' + file_name, 'futures-fb-daily', file_name)

    for campaign in list_of_ig_campaigns:

        insta_posts = d.get_posts(start_date_val, campaign[1], insta_api_token, terms, end_date_val)

        instagram_posts = []

        for i in insta_posts:
            try:
                description = i['description'].lower()
            except:
                description = ''
            platform = 'Instagram'
            author = i['account']['name']
            profile_img = i['account']['profileImage']
            user_name = i['account']['handle']
            reach = i['account']['subscriberCount']
            post_id = i['postUrl'].replace('https://www.instagram.com/p/', '').replace('/', '') # first part is user_id, but don't need
            time_stamp = i['date']
            text = description
            rts_shares = 0
            link = i['postUrl']
            faves_likes = i['statistics']['actual']['favoriteCount']
            comments = i['statistics']['actual']['commentCount']
            media_type = i['type']
            if media_type == 'photo' or media_type == 'album':
                try:
                    media_url = i['media'][0]['url']
                except:
                    media_url = ''
            elif media_type == 'video':
                try:
                    media_url = i['media'][1]['url'] # for vid it gives vid url first, then photo
                except: media_url = ''

            quot_text = ''
            quot_img = ''
            instagram_posts.append([platform,
                            author,
                            user_name,
                            reach,
                            post_id,
                            time_stamp,
                            text,
                            rts_shares,
                            faves_likes,
                            comments,
                            media_type,
                            link,
                            media_url,
                            quot_text,
                            quot_img,
                            profile_img
                            ])



        facebook_df = pd.DataFrame(columns= ['platform_name',
                                            'author',
                                            'user_name',
                                            'reach',
                                            'post_id',
                                            'time_stamp',
                                            'text',
                                            'rts_shares',
                                            'faves_likes',
                                            'comments',
                                            'media_type',
                                            'link',
                                            'media_url',
                                            'quot_text',
                                            'quot_img',
                                            'profile_img'],
                                data = instagram_posts)
        bytes_to_write = facebook_df.to_csv(None).encode()
        blob_client = blob_service_client.get_blob_client("philcontainertest", campaign[0] + "-" + str(datetime.date.today()) + "_accounts.csv")
        blob_client.upload_blob(bytes_to_write, blob_type="BlockBlob")

    ###################################
    #                                 #
    #      SEARCH EXPORT              #
    #                                 #
    ###################################

    man_u_search_terms = 'manchester united,man utd,red devil,old trafford,bruno fernandes,marcus rashford,ole solskj,alex ferguson,ed woodward,david de gea,victor lindel,harry maguire,paul pogba,edinson cavani,juan mata,anthony martial,mason greenwood,luke shaw,dean henderson,alex telles,wan bissaka,nemanja matic,van de beek,scott mcTominay'
    #formula_e_search_terms = 'abbFormulae, formula e, formule e, formel e, #abbformulae, #fiaformulae, @ABBFormulaE'
    supercars_search_terms = 'V8 Supercars, Repco Supercars, #RepcoSC, #V8SC, #Supercars, @Supercars, @supercarschampionship, #supercarschampionship, Red Bull Ampol, Erebus Motorsport, Kelly Racing, Kelly Grove, Irwin Racing, DeWalt Racing, Brad Jones Racing, Matt Stone Racing, Walkinshaw Andretti, Tickford Racing, DJR Racing, Triple Eight, Team 18, Team Sydney, Shane Van Ginsbergen, Nick Percat, Mark Winterbottom, Chaz Mostert, Jamie Whincup, Anton de Pasquale, Cam Waters, Jack Le Brocq'
    formula_e_search_terms = 'abbFormulae, #abbformulae, #fiaformulae, @ABBFormulaE, @fiaformulae, Formula E, Formel E, Formule E, #Diriyaheprix, @envisionvirginracing, @tagheuerporschefe, @dstecheetah, @mahindraracing'

    nrl_search_terms = "nrl, rugby league, state of origin, telstra premiership, magic round, peter v'lys, commission v'lys, rew abdo, abdo ceo, abdo executive, brisbane broncos, canterbury bulldogs, canberra raiders, cronulla sharks, melbourne storm, manly sea eagles, parramatta eels, south rabbitohs, wests tigers, queensl cowboys, penrith panthers, sydney roosters, zeal warriors, warriors tamworth, gold coast titans, newcastle knights, anz stadium rabbitohs, warriors mt smart stadium, anz stadium bulldogs, sydney cricket ground roosters, scg roosters, mcdonald jones stadium knights, broncos suncorp stadium, aami park storm, queensl country bank stadium cowboys, lottol sea eagles, cbus super stadium titans, gio stadium raiders, leichhardt oval tigers, netstrata jubilee stadium dragons, netstrata jubilee stadium sharks) OR (win stadium dragons, panthers stadium, broncos haas, broncos milford, broncos pangaii, broncos seibold, anthony seibold, broncos boyd, broncos glenn, titans arrow, titans taylor, ash taylor, titans proctor, titans holbrook, justin Holbrook, titans james, ryan james, mal meninga, titans meninga, roosters robinson, trent Robinson, cooper cronk, roosters cronk, james tedesco, roosters tedesco, boyd cordner, roosters cordner, roosters premiers, roosters keary, roosters morris, roosters crichton, roosters friend, roosters radley, victor radley, brad Arthur, eels arthur, clint gutherson, eels gutherson, blake Ferguson, eels ferguson, maika sivo, eels sivo, mitchell moses, eels moses, rugby rlc, rugby arl, bankwest stadium"

    emls_search_terms = '#eMLS, #eMLSCup, #eMLSSeries1, #eMLSSeries2, eMLS CUP, eMLS, @eMLS, @xbleu7, @didychrislito'

    lancashire_search_terms = 'emirates old Trafford , emiratesot , lancashire cricket , lancashire lightning , lancashire ccc , lancashire thunder , lancashire county cricket club , lancscricket, #RedRoseTogether, lccc , lancashirecricket , lancs CCC , lancsccc , lancashire'

    football_australia_terms = 'adelaide united  ,a-league  adelaide  united , a-league  adelaide  reds ,a-league  adelaide  united ,a-league  adelaide  reds , a-league  adelaide  united , a-league  adelaide  reds , ladder  adelaide  united , soccer  adelaide  united , w-league  adelaide  united , w-league  adelaide  reds , w-league  adelaide  united , w-league  adelaide  reds , w-league  adelaide  united , w-league  adelaide  reds , brisbane roar  ,a-league  brisbane  roar , a-league  roar , a-league  brisbane  roar , a-league  roar , ladder  brisbane  roar , soccer  brisbane  roar ,a-league  brisbane  roar ,a-league  roar , w-league  brisbane  roar , w-league  roar , w-league  brisbane  roar , w-league  roar , w-league  brisbane  roar , w-league   roar, central coast mariners  ,a-league  central  mariners , a-league  mariners , a-league  central  mariners , a-league  mariners , ladder  central  mariners , soccer  central  mariners ,a-league  central  mariners ,a-league  mariners , macarthur fc  ,a-league  macarthur  fc , a-league  macarthur , ladder  macarthur  fc , soccer  macarthur  fc , a-league  macarthur  fc , a-league  macarthur ,a-league  macarthur  fc ,a-league  macarthur ,  a-league  melbourne  city , a-league  city , ladder  melbourne  city , soccer  melbourne  city , a-league  melbourne  city , a-league  city ,a-league  melbourne  city ,a-league  city , w-league  melbourne  city , w-league  city , w-league  melbourne  city , w-league  city , w-league  melbourne  city , w-league  city , melbourne victory  ,a-league  melbourne  victory , a-league  victory , ladder  melbourne  victory , soccer  melbourne  victory , a-league  melbourne  victory , a-league  victory ,a-league  melbourne  victory ,a-league  victory , w-league  melbourne  victory , w-league  victory , w-league  melbourne  victory , w-league  victory , w-league  melbourne  victory , w-league  victory , newcastle jets  ,a-league  newcastle  jets , a-league  jets , ladder  newcastle  jets , soccer  newcastle  jets , a-league  newcastle  jets , a-league  jets ,a-league  newcastle  jets ,a-league  jets , w-league  newcastle  jets , w-league  jets , w-league  newcastle  jets , w-league  jets , w-league  newcastle  jets , w-league  jets , perth glory  ,a-league  perth  glory , a-league  glory , ladder  perth  glory , soccer  perth  glory , a-league  perth  glory , a-league  glory ,a-league  perth  glory ,a-league  glory , w-league  perth  glory , w-league  glory , w-league  perth  glory , w-league  glory , w-league  perth  glory , w-league  glory , sydney fc  ,a-league  sydney  fc , a-league  sky blues , ladder  sydney  fc , soccer  sydney  fc , a-league  sydney  fc , a-league  sky blues ,a-league  sydney  fc ,a-league  sky blues , ladder  sydney  fc , soccer  sydney  fc , w-league  sydney  fc , w-league  sydney  fc , w-league  sydney  fc , wellington phoenix  ,a-league  wellington  phoenix , a-league  phoenix , ladder  wellington  phoenix , soccer  wellington  phoenix , a-league  wellington  phoenix , a-league  phoenix ,a-league  wellington  phoenix ,a-league  phoenix , western sydney werers  ,a-league  western  werers , a-league  werers , ladder  western  werers , soccer  western  werers ,a-league  western  werers ,a-league  werers , a-league  western  werers , a-league  werers , w-league  western  werers , w-league  werers , w-league  western  werers , w-league  werers , w-league  western  werers , w-league  werers , western united  , a-league   western  united , ladder  western  united , soccer   western  united , a-league   western  united ,a-league   western  united'

    atl_falcons_terms = 'AtlantaFalcons , ATLUTD , MBStadium , ATLUTD2 , VAMOSATLUTD , thdbackyard , TheFactionATL , AcademyATLUTD , ResurganceATL , DirtySouthSoc , FootieMob , _moadams , AntonWalkes , _milesrobinson_ , JosefMartinez17 , atlutdpup , celebrationbowl , DEalesATLUTD , Francoeescobar , M_Ryan02 , MercedesBenzUSA , BocaBoca3 , CFAPEachBowl , ATLFalconsUK , CalvinRidley1 , TailgateTeam , #RiseUpATL , #Falcons , #ATLUTD , #AtlantaUnited , #AtlantaFalcons , #MattyIce , #UniteAndConquer , #FiveStripeFriday , #5StripeFriday , #MercedesBenzStadium , #MBStadium , #VamosATL , #ATLUnitedFC , #ATLUnited , #ATLSoccer , #ATLUTD , Falcons , Atlanta Falcons ,  Mercedes-Benz Stadium , Mercedes Benz Stadium ,  Atlanta United  , ATL United , @AtlantaFalcons , @ATLUTD , @MBStadium , @ATLUTD2 , @VamosATLUTD'

    all_str_terms = '@MLB , @AllStarGame , @TMobile , @Mastercard , @Gatorade , @chevrolet , @FreddieFreeman5 , @tatis_jr , @ronaldacunajr24 , @mookiebetts , @BusterPosey , @ozzie , @KrisBryant_23 , @JTRealmuto , @treavturner , @bcraw35 , @JuanSoto25_ , @B_Woody24 , @Max_Scherzer , @faridyu , @BauerOutage , @Kimbrel46 , @Mark_Melancon_ , @SalvadorPerez15 , @MikeTrout , @TheJudge44 , @GerritCole45 , @TeamCJCorrea , @JDMartinez28 , @ShaneBieber19 , @AChapman_105 , @Dbacks , @Braves , @Orioles , @RedSox , @whitesox , @Cubs , @Reds , @Indians , @Rockies , @tigers , @astros , @Royals , @Angels , @Dodgers , @Marlins , @Brewers , @Twins , @Yankees , @Mets , @Athletics , @Phillies , @Pirates , @Padres , @SFGiants , @Mariners , @Cardinals , @RaysBaseball , @Rangers , @BlueJays , @Nationals , #HRDerby , #AllStarBallot , #MLBVote , #StandUpToCancer , #SU2C , #AllStarGame , MLB , All Star Weekend , MLB All-Star Weekend , MLB All Star Game , MLB All-Star Game , T Mobile Home Run Derby , T-Mobile Home Run Derby , Gatorade All Star Workout Day , Gatorade All-Star Workout Day , All Star Game presented by Mastercard , All-Star Game presented by Mastercard , Chevrolet MLB All Star Game MVP , Chevrolet MLB All-Star Game MVP , Midsummer Classic , 2021 Midsummer Classic'

    packers_terms = 'Packers, Green Bay Pack, #GoPackGo , @packers , #packers'

    evo_terms = '#EVO2021 , EVO2021 , @EVO , EVO2021 , EVO Championship Series'

    search_list_of_campaigns = [
                                    ['ManUtd_2020-21-season', man_u_search_terms, 3],
                                    ['FormulaE_2021-season', formula_e_search_terms, 5],
                                    ['Supercars_2021-season', supercars_search_terms, 5, 'AU,NZ'],
                                    ['NRL_2021-season', nrl_search_terms, 5, 'AU,NZ'],
                                    ['PlayStation_2021-eMLS-Cup', emls_search_terms, 5],
                                    ['Lancashire_2021', lancashire_search_terms, 4],
                                    ['FootballAus_2020-21-Season', football_australia_terms, 5, 'AU,NZ'],
                                    ['ATLFalcons_2020-21-season', atl_falcons_terms, 3],
                                    ['Packers_2021-22-season', packers_terms, 5],
                                    ['EVO_2021-22-season', evo_terms, 5],
                                    ['MLB_AllStar_2021', all_str_terms, 5]
                                ]
    ## 3rd value in lists above is the number of days going back


    all_posts_list = []


    headers = {
    'Accept': 'application/json',
    'x-api-token': insta_api_token,
    }

    time.sleep(12)

    for camp in search_list_of_campaigns:
        offset = 0

        while offset <= 900:
            try:
            #print(offset)
                if len(camp) == 4:
                    params = (
                        ('count', 100),
                        ('offset', offset),
                        ('startDate', (datetime.date.today() - datetime.timedelta(camp[2])).isoformat()),
                        ('searchTerm', camp[1]),
                        ('sortBy', 'total_interactions'),
                        ('platforms', "facebook,instagram"),
                        ('pageAdminTopCountry', camp[3])
                    )
                else:
                    params = (
                        ('count', 100),
                        ('offset', offset),
                        ('startDate', (datetime.date.today() - datetime.timedelta(camp[2])).isoformat()),
                        ('searchTerm', camp[1]),
                        ('sortBy', 'total_interactions'),
                        ('platforms', "facebook,instagram")
                    )

                response = requests.get('https://api.crowdtangle.com/posts/search',
                                            headers=headers, params=params)
                resp = response.json()
                #print(len(resp['result']['posts']))

                for i in resp['result']['posts']:

                    indiv_post = []
                    campaign_name = camp[0]
                    platformId = i['platformId']
                    platform = i['platform']
                    date = i['date']
                    updated = i['updated']
                    post_type = i['type']
                    try:
                        post_description = i['description']
                    except:
                        post_description = ''
                    try:
                        message = i['message']
                    except:
                        message = ''
                    try:
                        link = i['link']
                    except:
                        link = ''
                    postUrl = i['postUrl']
                    subscriberCount = i['subscriberCount']
                    score = i['score']
                    try:
                        media_type = i['media'][0]['type']
                    except:
                        media_type = ''
                    try:
                        media_url = i['media'][0]['url']
                    except:
                        media_url = ''
                    try:
                        likeCount = i['statistics']['actual']['likeCount']
                    except:
                        like_count = likeCount = i['statistics']['actual']['favoriteCount']
                    try:
                        shareCount = i['statistics']['actual']['shareCount']
                    except:
                        shareCount = 0
                    try:
                        commentCount = i['statistics']['actual']['commentCount']
                    except:
                        commentCount = 0
                    account_id = i['account']['id']
                    account_name = i['account']['name']
                    account_subscriberCount = i['account']['subscriberCount']
                    account_url = i['account']['url']
                    try:
                        account_accountType = i['account']['accountType']
                    except:
                        account_accountType = ''

                    indiv_post.extend([
                                    campaign_name,
                                    platformId,
                                    platform,
                                    date,
                                    updated,
                                    post_type,
                                    post_description,
                                    message,
                                    link,
                                    postUrl,
                                    subscriberCount,
                                    score,
                                    media_type,
                                    media_url,
                                    likeCount,
                                    shareCount,
                                    commentCount,
                                    account_id,
                                    account_name,
                                    account_subscriberCount,
                                    account_url,
                                    account_accountType])
                    all_posts_list.append(indiv_post)
            # print(len(all_posts_list))
            # print('--')
                offset += 100
                time.sleep(12)
            except:
                offset += 100
                time.sleep(12)
                print('SEARCH EXCEPTION')
                print(offset)
                try:
                    print(response)
                except:
                    print('couldnt print response')

    posts_df = pd.DataFrame(data = all_posts_list,
                        columns = [
                        'campaign_name',
                        'platformId',
                        'platform',
                        'date',
                        'updated',
                        'post_type',
                        'post_description',
                        'message',
                        'link',
                        'postUrl',
                        'subscriberCount',
                        'score',
                        'media_type',
                        'media_url',
                        'likeCount',
                        'shareCount',
                        'commentCount',
                        'account_id',
                        'account_name',
                        'account_subscriberCount',
                        'account_url',
                        'account_accountType'])

    bytes_to_write = posts_df.to_csv(None).encode()
    blob_client = blob_service_client.get_blob_client("philcontainertest", "search_ig_and_fb" + "-" + str(datetime.date.today()) + "_search.csv")
    blob_client.upload_blob(bytes_to_write, blob_type="BlockBlob")



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    scrape_CrowdTangle()
