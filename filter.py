import pandas as pd

df = pd.read_csv('/Users/devalou/PycharmProjects/twitter_api_covid/fortune500_tweets.csv', header=None)
df.columns = ['company', 'author id', 'created_at', 'geo', 'id', 'lang', 'like_count', 'quote_count', 'reply_count',
              'retweet_count', 'source', 'tweet']
print(df.head())

df_filter = df[['company', 'created_at', 'source', 'tweet']]
print(df_filter.head())