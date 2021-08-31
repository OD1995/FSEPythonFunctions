import json
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import numpy as np
import math
import time
import datetime

api_url = 'https://api.crowdtangle.com/'

class Dashboard:

        def __init__(self, api_url):
            self.api_url = api_url
            #self.api_token = api_token

        def get_lists(self, api_token):
            headers = {
            'Accept': 'application/json',
            'x-api-token': api_token,
            }
            params = ()

            response = requests.get(api_url + '/lists',
                                headers=headers, params=params)

            lists = response.json()
            return lists
        # def get_leaderboard(self, list_id):
        #     headers = {
        #     'Accept': 'application/json',
        #     'x-api-token': api_token,
        #     }
        #     params = (
        #         ('listId', list_id),
        #     )
        #
        #     response = requests.get(api_url + '/leaderboard',
        #                         headers=headers, params=params)
        #
        #     leaderboards = response.json()
        #
        #     return leaderboards
###############################

        def initial_leaderboard_call(self, offset, list_id, api_token):
                headers = {
                'Accept': 'application/json',
                'x-api-token': api_token,
                }
                params = (
                    ('listId', list_id),
                    ('offset', offset),
                    ('startDate', (datetime.date.today() - datetime.timedelta(90)).isoformat())
                )

                response = requests.get(api_url + '/leaderboard',
                                            headers=headers, params=params)
                print(params)
                status_code = response.status_code
                if response.status_code == 200:
                    leaderboards = response.json()
                else:
                    print('ERROR')

                    print(response.status_code)
                    leaderboards = None
               # print(posts)
                return leaderboards, status_code

        def get_leaderboards(self, list_id, api_token):
            offset = 0
            #posts = initial_post_call(api_token, offset)
            leaderboards, status_code = Dashboard.initial_leaderboard_call(self, offset, list_id, api_token)
            board_data = leaderboards['result']['accountStatistics']
            print('length ' + str(len(board_data)))
            len_last_pull = len(board_data)
            #print(posts['result'])
            while status_code == 200 and len_last_pull != 0:
                try:
                    offset += 50
                    print(offset)
                    time.sleep(11) # can only make 6 calls a min to the api
                    leaderboards, status_code = Dashboard.initial_leaderboard_call(self, offset, list_id, api_token)

                    next_page = leaderboards['result']['accountStatistics']
                    len_last_pull = len(next_page)
                    print('last = ' + str(len_last_pull))
                    #print(next_page)
                    print('-------------------------------------------------------------')

                    board_data += next_page
                    print('length ' + str(len(board_data)))

                except:
                    print('except end')
                    break
            return board_data





##################################
        def initial_post_call(self, offset, start_date, list_id, api_token, terms, end_date = 'now'):
                headers = {
                'Accept': 'application/json',
                'x-api-token': api_token,
                }
                if end_date != 'now':
                    params = (
                        ('listIds', list_id),
                        ('count', '100'), # max number you can pull
                        ('startDate', start_date),
                        ('endDate', end_date),
                        ('offset', offset),
                        ('sortBy', 'date'),
                        ('searchTerm', terms)
                        #('startDate', (datetime.date.today() - datetime.timedelta(90)).isoformat()

                    )
                else:
                    params = (
                        ('listIds', list_id),
                        ('count', '100'), # max number you can pull
                        ('startDate', start_date),
                        ('offset', offset),
                        ('sortBy', 'date'),
                        ('searchTerm', terms)
                            )
                response = requests.get(api_url + '/posts',
                                            headers=headers, params=params)
                print(params)
                status_code = response.status_code
                if response.status_code == 200:
                    posts = response.json()
                else:
                    print('ERROR')

                    print(response.status_code)
                    posts = None
               # print(posts)
                return posts, status_code

        def get_posts(self, start_date, list_id, api_token, terms, end_date = 'now'):
            offset = 0 #100
            #posts = initial_post_call(api_token, offset)
            posts, status_code = Dashboard.initial_post_call(self, offset, start_date, list_id, api_token, terms, end_date)
            page_data = posts['result']['posts']
            print('length ' + str(len(page_data)))
            len_last_pull = len(page_data)
            #print(posts['result'])
            while status_code == 200 and len_last_pull != 0:
                try:
                    offset += 100
                    time.sleep(11) # can only make 6 calls a min to the api
                    print(offset)
                    posts, status_code = Dashboard.initial_post_call(self, offset, start_date,  list_id, api_token, terms, end_date)

                    next_page = posts['result']['posts']
                    len_last_pull = len(next_page)
                    print('last = ' + str(len_last_pull))
                    #print(next_page)
                    print('-------------------------------------------------------------')

                    page_data += next_page
                    print('length ' + str(len(page_data)))

                except:
                    print('except end')
                    break
            return page_data
