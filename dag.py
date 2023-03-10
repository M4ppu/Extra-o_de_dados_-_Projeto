import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import datetime 
import pandas as pd
import requests
import json
import time

def get_article():
    apikey = 'e023b73a1bac6a32778b9a1e143a365a'
    url = 'https://gnews.io/api/v4'
    endpoint = '/search'
    parameters = {
        'apikey': apikey,
        'q': 'tubarão',
        'lang': 'pt',
        'country': 'br'
    }
    x = requests.get(url + endpoint, params = parameters)
    if x.status_code >= 500:
        time.sleep(1)
        x = requests.get(url + endpoint, params = parameters)
        if x.status_code >= 500:
            raise Exception('internal error ' + str(x.status_code))

    df = pd.DataFrame(x.json()['articles'])
    df['name'] = df.apply(lambda x: x['source']['name'], axis = 1)
    df = df.drop(columns = ['description', 'content', 'source'])
    df['publishedAt'] =  pd.to_datetime(df['publishedAt'], format='%Y-%m-%dT%H:%M:%SZ')
    today = datetime.now()
    df['year'] = today.year
    df['month'] = today.month
    df['day'] = today.day
    df.to_parquet('article.parquet', partition_cols=['year', 'month', 'day'])
    
def get_sentiment():
    today = datetime.now()
    year = today.year
    month = today.month
    day = today.day
    df = pd.read_parquet(f'article.parquet/year={year}/month={month}/day={day}')
    apikey = '1d5898219f7c5d3de72e543775524cb5'
    url = 'https://api.meaningcloud.com'
    endpoint = '/sentiment-2.1'
    parameters = {
        'key': apikey,
        'lang': 'pt',
        'txt': '2023 é ano com mais ataques de tubarão desde 2006 em Pernambuco'
    }
    x = requests.post(url + endpoint, params = parameters)
    
        

with DAG(
    dag_id="collect_articles",
    schedule='0 0 * * *',
    start_date=airflow.utils.dates.days_ago(1)
) as dag:
    