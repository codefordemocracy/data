import logging
import sys
from scrapinghub import ScrapinghubClient
from google.cloud import secretmanager
import pandas as pd
import numpy as np
import json
from urllib.parse import urlparse
from google.cloud import pubsub

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
scrapy_api_key = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/scrapy_api_key/versions/1"}).payload.data.decode()
scrapy_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/scrapy_project_id/versions/1"}).payload.data.decode()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
publisher = pubsub.PublisherClient()

# scrapy cloud settings
client = ScrapinghubClient(scrapy_api_key)
project = client.get_project(int(scrapy_project_id))

# utility function to return a dataframe of data from a job
def get_job(key):
    job = project.jobs.get(key)
    if job.metadata.get('state') != 'finished':
        logger.error(' - '.join(['VALIDATION FAIL', job.metadata.get('spider'), job.key, 'run not finished']))
        sys.exit()
    elif job.metadata.get('scrapystats')['finish_reason'] != 'finished':
        logger.error(' - '.join(['VALIDATION FAIL', job.metadata.get('spider'), job.key, 'incomplete run']))
        sys.exit()
    elif 'log_count/ERROR' in job.metadata.get('scrapystats'):
        logger.error(' - '.join(['VALIDATION FAIL', job.metadata.get('spider'), job.key, 'run has errors']))
        sys.exit()
    else:
        logger.info(' - '.join(['STATUS', job.metadata.get('spider'), job.key, 'retrieving scraped items']))
        df = pd.DataFrame()
        for item in job.items.iter():
            row = pd.DataFrame.from_records([item])
            df = pd.concat([df, row], ignore_index=True)
        return df

# get sources from the allsides and mediabiasfactcheck scrapers and queue for processing
def news_sources_ingest_get_crawls(message, context):

    # get job keys from the Pub/Sub message
    allsides = message['attributes']['allsides']
    mediabiasfactcheck = message['attributes']['mediabiasfactcheck']

    # grab data from previous runs
    df_allsides = get_job(allsides)
    df_mediabiasfactcheck = get_job(mediabiasfactcheck)

    logger.info(' - '.join(['STATUS', 'cleaning scraped items']))

    # clean the allsides data
    df_allsides['AllSides Source'] = df_allsides['Source'].str.strip()
    df_allsides['Bias'] = df_allsides['Bias'].str.strip()
    df_allsides['Bias'] = df_allsides['Bias'].str.lower()
    df_allsides = df_allsides[df_allsides['Bias'] != 'mixed']
    df_allsides['Website'] = df_allsides['Website'].str.strip()
    df_allsides['Website'] = df_allsides['Website'].str.lower()
    df_allsides['Domain'] = df_allsides['Website'].apply(lambda x: urlparse(x).netloc)
    df_allsides['Domain'] = df_allsides['Domain'].str.replace('www.','')
    df_allsides['Domain'] = df_allsides['Domain'].apply(lambda x: x.rsplit(',',1)[0])
    df_allsides['Domain'] = df_allsides['Domain'].apply(lambda x: x[1:] if x.startswith('.') else x)
    df_allsides = df_allsides[df_allsides['Domain'] != '']

    # clean the mediabiasfactcheck data
    df_mediabiasfactcheck['MBFC Source'] = df_mediabiasfactcheck['Source'].str.strip()
    df_mediabiasfactcheck['Bias'] = df_mediabiasfactcheck['Bias'].str.strip()
    df_mediabiasfactcheck['Bias'] = df_mediabiasfactcheck['Bias'].str.lower()
    df_mediabiasfactcheck['Images'] = df_mediabiasfactcheck['Images'].str.strip()
    df_mediabiasfactcheck['Images'] = df_mediabiasfactcheck['Images'].str.lower()
    df_mediabiasfactcheck['Tags'] = df_mediabiasfactcheck['Tags'].apply(lambda x: str(x))
    df_mediabiasfactcheck['Tags'] = df_mediabiasfactcheck['Tags'].str.lower()
    df_mediabiasfactcheck['Website'] = df_mediabiasfactcheck['Website'].str.strip()
    df_mediabiasfactcheck['Website'] = df_mediabiasfactcheck['Website'].str.lower()
    df_mediabiasfactcheck['Domain'] = df_mediabiasfactcheck['Website'].apply(lambda x: urlparse(x).netloc)
    df_mediabiasfactcheck['Domain'] = df_mediabiasfactcheck['Domain'].str.replace('www.','')
    df_mediabiasfactcheck['Domain'] = df_mediabiasfactcheck['Domain'].apply(lambda x: x.rsplit(',',1)[0])
    df_mediabiasfactcheck['Domain'] = df_mediabiasfactcheck['Domain'].apply(lambda x: x[1:] if x.startswith('.') else x)
    df_mediabiasfactcheck = df_mediabiasfactcheck[df_mediabiasfactcheck['Domain'] != '']

    # assign bias scores
    df_allsides.loc[df_allsides['Bias'] == 'left', 'AllSides Bias Score'] = -2
    df_allsides.loc[df_allsides['Bias'] == 'lean left', 'AllSides Bias Score'] = -1
    df_allsides.loc[df_allsides['Bias'] == 'center', 'AllSides Bias Score'] = 0
    df_allsides.loc[df_allsides['Bias'] == 'lean right', 'AllSides Bias Score'] = 1
    df_allsides.loc[df_allsides['Bias'] == 'right', 'AllSides Bias Score'] = 2
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Bias'].str.contains('left bias'), 'MBFC Bias Score'] = -2
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Bias'].str.contains('left-center bias'), 'MBFC Bias Score'] = -1
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Bias'].str.contains('pro-science'), 'MBFC Bias Score'] = 0
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Bias'].str.contains('least biased'), 'MBFC Bias Score'] = 0
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Bias'].str.contains('right-center bias'), 'MBFC Bias Score'] = 1
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Bias'].str.contains('right bias'), 'MBFC Bias Score'] = 2
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Bias'].str.contains('questionable source'), 'MBFC Factually Questionable Flag'] = 1
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Bias'].str.contains('conspiracy-pseudoscience'), 'MBFC Factually Questionable Flag'] = 1
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Bias'].str.contains('conspiracy-pseudoscience'), 'MBFC Conspiracy Flag'] = 1
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Tags'].str.contains('hate'), 'MBFC Hate Group Flag'] = 1
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Tags'].str.contains('propaganda'), 'MBFC Propaganda Flag'] = 1
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['Bias'].str.contains('satire'), 'MBFC Satire Flag'] = 1
    df_mediabiasfactcheck.loc[(df_mediabiasfactcheck['MBFC Factually Questionable Flag'] == 1) & (df_mediabiasfactcheck['Images'].str.contains('left') | df_mediabiasfactcheck['Tags'].str.contains('left')), 'MBFC Bias Score'] = -3
    df_mediabiasfactcheck.loc[(df_mediabiasfactcheck['MBFC Factually Questionable Flag'] == 1) & (df_mediabiasfactcheck['Images'].str.contains('right') | df_mediabiasfactcheck['Tags'].str.contains('right')), 'MBFC Bias Score'] = 3
    df_mediabiasfactcheck.loc[(df_mediabiasfactcheck['MBFC Conspiracy Flag'] == 1) & (df_mediabiasfactcheck['Images'].str.contains('left') | df_mediabiasfactcheck['Tags'].str.contains('left')), 'MBFC Bias Score'] = -3
    df_mediabiasfactcheck.loc[(df_mediabiasfactcheck['MBFC Conspiracy Flag'] == 1) & (df_mediabiasfactcheck['Images'].str.contains('right') | df_mediabiasfactcheck['Tags'].str.contains('right')), 'MBFC Bias Score'] = 3
    df_mediabiasfactcheck.loc[(df_mediabiasfactcheck['MBFC Hate Group Flag'] == 1) & (df_mediabiasfactcheck['Images'].str.contains('left') | df_mediabiasfactcheck['Tags'].str.contains('left')), 'MBFC Bias Score'] = -3
    df_mediabiasfactcheck.loc[(df_mediabiasfactcheck['MBFC Hate Group Flag'] == 1) & (df_mediabiasfactcheck['Images'].str.contains('right') | df_mediabiasfactcheck['Tags'].str.contains('right')), 'MBFC Bias Score'] = 3
    df_mediabiasfactcheck.loc[(df_mediabiasfactcheck['MBFC Propaganda Flag'] == 1) & (df_mediabiasfactcheck['Images'].str.contains('left') | df_mediabiasfactcheck['Tags'].str.contains('left')), 'MBFC Bias Score'] = -3
    df_mediabiasfactcheck.loc[(df_mediabiasfactcheck['MBFC Propaganda Flag'] == 1) & (df_mediabiasfactcheck['Images'].str.contains('right') | df_mediabiasfactcheck['Tags'].str.contains('right')), 'MBFC Bias Score'] = 3
    df_mediabiasfactcheck.loc[df_mediabiasfactcheck['MBFC Satire Flag'] == 1, 'MBFC Bias Score'] = None

    # create master list of sources and bias ratings
    df_allsides = df_allsides[['Domain', 'AllSides Source', 'AllSides Bias Score']]
    df_mediabiasfactcheck = df_mediabiasfactcheck[['Domain', 'MBFC Source', 'MBFC Bias Score', 'MBFC Factually Questionable Flag', 'MBFC Conspiracy Flag', 'MBFC Satire Flag', 'MBFC Hate Group Flag', 'MBFC Propaganda Flag']]
    df_sources = pd.merge(df_allsides, df_mediabiasfactcheck, on='Domain', how='outer')
    df_sources['Source'] = df_sources['AllSides Source']
    df_sources.loc[df_sources['MBFC Source'].isnull() == False, 'Source'] = df_sources['MBFC Source']
    df_sources['AllSides Bias Score'] = df_sources['AllSides Bias Score'].fillna("")
    df_sources['MBFC Bias Score'] = df_sources['MBFC Bias Score'].fillna("")
    df_sources['MBFC Factually Questionable Flag'] = df_sources['MBFC Factually Questionable Flag'].fillna(0)
    df_sources['MBFC Conspiracy Flag'] = df_sources['MBFC Conspiracy Flag'].fillna(0)
    df_sources['MBFC Satire Flag'] = df_sources['MBFC Satire Flag'].fillna(0)
    df_sources['MBFC Hate Group Flag'] = df_sources['MBFC Hate Group Flag'].fillna(0)
    df_sources['MBFC Propaganda Flag'] = df_sources['MBFC Propaganda Flag'].fillna(0)

    # sort final list of sources
    df_sources = df_sources.sort_values(by=['Domain'])
    df_sources = df_sources.loc[df_sources['Domain'] != "facebook.com"]
    df_sources = df_sources.loc[df_sources['Domain'] != "instagram.com"]
    df_sources = df_sources.loc[df_sources['Domain'] != "youtube.com"]
    df_sources = df_sources.loc[df_sources['Domain'] != "medium.com"]

    logger.info(' - '.join(['COMPLETED', 'finished cleaning scraped items']))

    # send a message to Pub/Sub with doc of source to verify
    topic = 'projects/' + gcp_project_id + '/topics/news_sources_ingest_verify_domain'
    for index, row in df_sources.iterrows():
        doc = {
            "name": row["Source"],
            "domain": row["Domain"]
        }
        if row["AllSides Bias Score"] != "":
            doc["allsides"] = {
                "bias_score": row["AllSides Bias Score"]
            }
        if row["MBFC Bias Score"] != "":
            doc["mediabiasfactcheck"] = {
                "bias_score": row["MBFC Bias Score"],
                "factually_questionable_flag": row["MBFC Factually Questionable Flag"],
                "conspiracy_flag": row["MBFC Conspiracy Flag"],
                "satire_flag": row["MBFC Satire Flag"],
                "hate_group_flag": row["MBFC Hate Group Flag"],
                "propaganda_flag": row["MBFC Propaganda Flag"]
            }
        publisher.publish(topic, b'source data obtained', doc=json.dumps(doc))
    logger.info(' - '.join(['COMPLETED', 'sources sent to news_sources_ingest_verify_domain queue', str(len(df_sources.index))]))

    # return the number of processed sources
    return len(df_sources.index)
