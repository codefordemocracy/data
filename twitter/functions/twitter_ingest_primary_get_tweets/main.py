import logging
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import pubsub
from elasticsearch import Elasticsearch, helpers
import tweepy
import datetime
import json
import copy
from azure.cosmosdb.table.tableservice import TableService
import utilities

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
elastic_host = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_host/versions/1"}).payload.data.decode()
elastic_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_username_data/versions/1"}).payload.data.decode()
elastic_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_password_data/versions/1"}).payload.data.decode()
azure_account_name_twitter = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/azure_account_name_twitter/versions/1"}).payload.data.decode()
azure_account_key_twitter = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/azure_account_key_twitter/versions/1"}).payload.data.decode()
twitter_client_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/twitter_client_id/versions/1"}).payload.data.decode()
twitter_client_secret = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/twitter_client_secret/versions/1"}).payload.data.decode()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
table_service = TableService(account_name=azure_account_name_twitter, account_key=azure_account_key_twitter)
db = firestore.Client()
publisher = pubsub.PublisherClient()

# authenticate Twitter API
auth = tweepy.OAuthHandler(twitter_client_id, twitter_client_secret)
api = tweepy.API(auth, retry_count=1)

# get hydrated tweet information of a list of tweet ids and update Firestore, Table Storage, and ElasticSearch
def twitter_ingest_primary_get_tweets(message, context):

    # get the chunk of ids from the Pub/Sub message
    chunk1 = json.loads(message['attributes']['chunk1'])
    chunk2 = json.loads(message['attributes']['chunk2'])
    chunk3 = json.loads(message['attributes']['chunk3'])
    chunk = chunk1 + chunk2 + chunk3

    # set up Firestore refs
    primary_tweet_ref = db.collection('twitter').document('tweets').collection('primary')
    secondary_user_ref = db.collection('twitter').document('users').collection('secondary')

    logger.info(' - '.join(['PROCESSING TWEETS', str(chunk)]))

    # get tweet objects from Twitter API
    try:
        tweets = api.statuses_lookup(chunk, include_entities=True, tweet_mode="extended", map_=True)
    except tweepy.TweepError as e:
        logger.error(' - '.join(['ERROR WHEN HYDRATING TWEETS', str(e)]))
        tweets = None

    if tweets:
        actions = []
        for tweet in tweets:
            if not hasattr(tweet,'author'):
                #if tweet can't be hydrated, delete it from firestore
                primary_tweet_ref.document(str(tweet.id)).delete()
                logger.info(' - '.join(['DELETING TWEET FROM FIRESTORE', str(tweet.id)]))
                continue
            else:
                # parse the tweet
                records = utilities.parse_tweets(tweet)

                # process main tweet
                record = records["main"]
                if record["is_retweet"] is False:
                    # add to ElasticSearch
                    doc = copy.deepcopy(record)
                    doc.pop('hydrated', None)
                    doc["has_nlp"] = False
                    doc["in_graph"] = False
                    doc["obj"]["entities"] = json.loads(doc["obj"]["entities"])
                    if "place" in doc["obj"]:
                        doc["obj"]["place"] = json.loads(doc["obj"]["place"])
                    actions.append({
                        "_op_type": "index",
                        "_index": "twitter_tweets",
                        "_id": doc["id"],
                        "_source": doc
                    })
                    logger.info(' - '.join(['TWEET ADDED TO ELASTICSEARCH ACTIONS', str(doc["id"])]))
                    # process article links
                    for url in doc['obj']['entities']['urls']:
                        if 'twitter.com' not in url['expanded_url']:
                            # send article to be scraped
                            topic = 'projects/' + gcp_project_id + '/topics/news_articles_ingest_get_url'
                            publisher.publish(topic, b'scrape this url', url=url['expanded_url'])
                            logger.info(' - '.join(['INFO', 'url sent to news_articles_ingest_get_url queue', url['expanded_url']]))
                else:
                    logger.info(' - '.join(['TWEET SKIPPED', 'retweet', str(record["id"])]))

                # process tweets
                for record in records["tweets"]:
                    relatedTweetId = str(record["id"])
                    # add to Firestore
                    primary_tweet_ref.document(relatedTweetId).set(record)
                    logger.info(' - '.join(['ADDED', 'tweet', relatedTweetId]))

                # process users
                for record in records["users"]:
                    userId = str(record["id"])
                    # add to Firestore
                    u = secondary_user_ref.document(userId).get()
                    if not u.exists:
                        secondary_user_ref.document(userId).set(record)
                        logger.info(' - '.join(['ADDED', 'user', userId]))

                # process retweet relationships
                for record in records["retweets"]:
                    # add to ElasticSearch
                    actions.append({
                        "_op_type": "index",
                        "_index": "twitter_retweets",
                        "_id": str(record["source"])+"_"+str(record["target"]),
                        "_source": record
                    })
                    logger.info(' - '.join(['ADDED TO ELASTICSEARCH ACTIONS', 'retweet', str(record["source"]), str(record["target"])]))

                # process quote relationships
                for record in records["quotes"]:
                    # add to ElasticSearch
                    actions.append({
                        "_op_type": "index",
                        "_index": "twitter_quotes",
                        "_id": str(record["source"])+"_"+str(record["target"]),
                        "_source": record
                    })
                    logger.info(' - '.join(['ADDED TO ELASTICSEARCH ACTIONS', 'quote', str(record["source"]), str(record["target"])]))

                # process reply relationship
                for record in records["replies"]:
                    # add to ElasticSearch
                    actions.append({
                        "_op_type": "index",
                        "_index": "twitter_replies",
                        "_id": str(record["source"])+"_"+str(record["target"]),
                        "_source": record
                    })
                    logger.info(' - '.join(['ADDED TO ELASTICSEARCH ACTIONS', 'reply', str(record["source"]), str(record["target"])]))

                record = records["main"]
                # update Table Storage
                row = utilities.create_tweet_entity(record)
                table_service.insert_or_merge_entity('tweets', row)
                logger.info(' - '.join(['TWEET SYNCED TO TABLE STORAGE', str(record["id"])]))

        # bulk update elasticsearch
        helpers.bulk(es, actions)
        logger.info(' - '.join(['TWEETS SYNCED TO ELASTICSEARCH', str(len(actions))]))

        for tweet in tweets:
            # delete from Firestore
            primary_tweet_ref.document(str(tweet.id)).delete()
            logger.info(' - '.join(['COMPLETED', 'tweet deleted from Firestore', str(tweet.id)]))

    logger.info(' - '.join(['TWEETS UPDATED', str(len(chunk))]))

    # return number of processed tweets
    return len(chunk)
