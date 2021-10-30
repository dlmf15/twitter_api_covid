import requests
import os
import json
import pandas as pd
import csv
import datetime
import dateutil.parser
import time



def auth():
    return os.getenv('TOKEN')


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(keyword, start_date, end_date, max_results=10):
    search_url = "https://api.twitter.com/2/tweets/search/all"  # Change to the endpoint you want to collect data from

    # change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return search_url, query_params


def connect_to_endpoint(url, headers, params, next_token=None):
    params['next_token'] = next_token  # params object received from create_url function
    response = requests.request("GET", url, headers=headers, params=params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:

        if response.status_code == 429:
            buffer_wait_time = 15
            print("x-rate-limit-limit:", int(response.headers["x-rate-limit-limit"]))
            print("x-rate-limit-remaining:", int(response.headers["x-rate-limit-remaining"]))
            print("x-rate-limit-reset:", int(response.headers["x-rate-limit-reset"]))
            print(response.text)
            #resume_time = datetime.fromtimestamp(int(response.headers["x-rate-limit-reset"]) + buffer_wait_time)
            #print(f"Too many requests. Waiting on Twitter.\n\tResume Time: {resume_time}")
            #pause_until(resume_time)
        else:
            raise Exception(response.status_code, response.text)
    return response.json()


def append_to_csv(json_response, file_name, company):
    # A counter variable
    counter = 0

    # Open OR create the target CSV file
    file = open(file_name, 'a', newline='', encoding='utf-8')
    writer = csv.writer(file)

    # Loop through each tweet
    for tweet in json_response['data']:
        company = company
        author_id = tweet['author_id']
        created_at = dateutil.parser.parse(tweet['created_at'])

        if ('geo' in tweet):
            geo = tweet['geo']['place_id']
        else:
            geo = " "

        tweet_id = tweet['id']
        lang = tweet['lang']
        retweet_count = tweet['public_metrics']['retweet_count']
        reply_count = tweet['public_metrics']['reply_count']
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']
        source = tweet['source']
        text = tweet['text']
        res = [company, author_id, created_at, geo, tweet_id, lang, like_count, quote_count, reply_count, retweet_count,
               source, text]

        writer.writerow(res)
        counter += 1

    file.close()


# Inputs for the request
df = pd.read_csv("/Users/devalou/PycharmProjects/twitter_api_covid/originalBD_company_name.csv", delimiter=';')
company_list = df['company names'].tolist()

csv_file = open('originalBD_tweets_1_2.csv', 'a', newline='', encoding='utf-8')
csv_writer = csv.writer(csv_file)
csv_writer.writerow(['company', 'author id', 'created_at', 'geo', 'id', 'lang', 'like_count', 'quote_count',
                     'reply_count', 'retweet_count', 'source', 'tweet'])
csv_file.close()

for company in company_list[386:1000]:
    bearer_token = auth()
    headers = create_headers(bearer_token)
    company = '"' + company + '"'
    keyword = company + ' ("COVID" OR "COVID-19" OR "COVID19" OR "COVID 19" OR "CORONAVIRUS" OR "CORONAVIRUS-19" OR "CORONAVIRUS19" OR "CORONAVIRUS 19") ("relief" OR "aid" OR "assistance" OR "rescue" OR "succor") -"stock quote" -"stock quotes" -"quote prices" -"stock market" -"share price" -"stock price" -"stock exchange" -"bourse" -"securities exchange" -"financial results" -is:retweet lang:en'

    start_time = ['2020-03-01T00:00:00.000Z', '2020-04-01T00:00:00.000Z',
                  '2020-05-01T00:00:00.000Z', '2020-06-01T00:00:00.000Z',
                  '2020-07-01T00:00:00.000Z', '2020-08-01T00:00:00.000Z',
                  '2020-09-01T00:00:00.000Z', '2020-10-01T00:00:00.000Z',
                  '2020-11-01T00:00:00.000Z', '2020-12-01T00:00:00.000Z']

    end_time = ['2020-03-31T00:00:00.000Z', '2020-04-30T00:00:00.000Z',
                '2020-05-31T00:00:00.000Z', '2020-06-30T00:00:00.000Z',
                '2020-07-31T00:00:00.000Z', '2020-08-31T00:00:00.000Z',
                '2020-09-30T00:00:00.000Z', '2020-10-31T00:00:00.000Z',
                '2020-11-30T00:00:00.000Z', '2020-12-31T00:00:00.000Z']

    max_results = 500

    total_tweets = 0

    for month in range(0, 10):

        print('---------------------------------')
        print("Company: ", company)
        print("Month: ", month + 3)

        count = 0
        max_tweets_count = 10000
        next_token = None
        token = True

        while token:

            if count >= max_tweets_count:
                print("Month {} reach 10000 results.".format(month + 3))
                break

            time.sleep(5)

            url = create_url(keyword, start_time[month], end_time[month], max_results)
            json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
            result_count = json_response['meta']['result_count']
            print("Result Count: {}, Token: {}".format(result_count, next_token))

            # if next_token exits
            if 'next_token' in json_response['meta']:
                next_token = json_response['meta']['next_token']
                print('Next Token founded: ', next_token)

                if result_count is not None and result_count > 0 and next_token is not None:
                    append_to_csv(json_response, "originalBD_tweets_1_2.csv", company)
                    count += result_count
                    print("{} tweets appended!".format(result_count))
                    time.sleep(5)


            # if next_token not exit()
            else:
                if result_count is not None and result_count > 0:
                    append_to_csv(json_response, "originalBD_tweets_1_2.csv", company)
                    count += result_count
                    print("{} tweets appended!".format(result_count))
                    time.sleep(5)

                next_token = None
                token = False

            print("Month {} total tweets: {}".format(month + 3, count))
