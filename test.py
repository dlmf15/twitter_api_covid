import pandas as pd

df = pd.read_csv("/Users/devalou/PycharmProjects/twitter_api_covid/originalBD_company_name.csv", delimiter=';' )
original_bd = df['company names'].tolist()
print(len(original_bd))





