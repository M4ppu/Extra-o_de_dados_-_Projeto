import airflow
from airflow import DAG
from airflow.decorators import task
from datetime import datetime 
import pandas as pd
import requests
import json
import time


# Operação para capturar artigos
@task(task_id="get_article")
def get_article(q, lang, country):
    
    # Parâmetros
    apikey = ''
    url = 'https://gnews.io/api/v4'
    endpoint = '/search'
    parameters = {
        'apikey': apikey,
        'q': q,
        'lang': lang,
        'country': country
    }

    # Chamando API
    x = requests.get(url + endpoint, params = parameters)

    # Tolerância de erros  ao servidor (500+)
    if x.status_code >= 500:
        time.sleep(1)
        x = requests.get(url + endpoint, params = parameters)
        if x.status_code >= 500:
            raise Exception('internal error ' + str(x.status_code))

    # Capturando artigos e limpando DataFrame
    df = pd.DataFrame(x.json()['articles'])
    df['name'] = df.apply(lambda x: x['source']['name'], axis = 1)
    df = df.drop(columns = ['description', 'content', 'source'])
    df['publishedAt'] =  pd.to_datetime(df['publishedAt'], format='%Y-%m-%dT%H:%M:%SZ')
    df['lang'] = lang

    # Criando colunas de partição
    today = datetime.now()
    df['year'] = today.year
    df['month'] = today.month
    df['day'] = today.day

    # Salvando em arquivo parquet
    df.to_parquet('/tmp/data/article.parquet', partition_cols=['year', 'month', 'day'])


# Operação para analisar sentimento dos artigos
@task(task_id="get_sentiment")
def get_sentiment():
    
    # Carregando artigos (DataFrame)
    today = datetime.now()
    df = pd.read_parquet(f'/tmp/data/article.parquet/year={today.year}/month={today.month}/day={today.day}')
    
    # Parâmetros
    apikey = ''
    url = 'https://api.meaningcloud.com'
    endpoint = '/sentiment-2.1'
    parameters = {
        'key': apikey,
        'lang': '',
        'txt': ''
    }

    # Criando colunas de partição
    df['year'] = today.year
    df['month'] = today.month
    df['day'] = today.day

    #Criando colunas de sentimento
    df['sentiment'] = ''
    df['irony'] = ''
    df['confidence'] = 0
    
    # Chamando API
    for index, row in df.iterrows():
        parameters['txt'] = row['title']
        parameters['lang'] = row['lang']
        x = requests.post(url + endpoint, params = parameters)
        
        # Tolerância de erros  ao servidor (500+)
        if x.status_code >= 500:
            time.sleep(1)
            x = requests.get(url + endpoint, params = parameters)
            if x.status_code >= 500:
                raise Exception('internal error ' + str(x.status_code))
        x = x.json()

        # Preenchenco coluna de sentimentos
        df.loc[index, 'sentiment'] = x['score_tag']
        df.loc[index, 'irony'] = x['irony']
        df.loc[index, 'confidence'] = x['confidence']
        time.sleep(1)
    
    # Limpando DataFrame
    sentiments = {
    'P+': 'strong positive',
    'P': 'positive',
    'NEU': 'neutral',
    'N': 'negative',
    'N+': 'strong negative',
    'NONE': 'without polarity'
}
    df['sentiment'] = df.sentiment.map(sentiments)

    # Salvando em arquivo parquet
    df.to_parquet('/tmp/data/article_sentiment.parquet', partition_cols=['year', 'month', 'day'])

# Criando e configurando objeto Dag
with DAG(
    dag_id="collect_articles",
    schedule='0 0 * * *',
    start_date=airflow.utils.dates.days_ago(1)
) as dag:

    # Chamando as funções
    get_article_run_1 = get_article('tubarão', 'pt', 'br')
    get_article_run_2 = get_article('shark', 'en', 'us')
    get_sentiment_run = get_sentiment()

    # Ditando ordem de execução
    get_article_run_1 >> get_article_run_2 >> get_sentiment_run