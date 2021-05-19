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

# get timeline tweets from user id and update Firestore, Table Storage, and ElasticSearch
def twitter_ingest_primary_get_timeline(message, context):

    # get id from the Pub/Sub message
    id = int(message['attributes']['id'])

    # set up Firestore refs
    primary_user_ref = db.collection('twitter').document('users').collection('primary')
    primary_tweet_ref = db.collection('twitter').document('tweets').collection('primary')
    secondary_user_ref = db.collection('twitter').document('users').collection('secondary')

    # grab latest cursor data
    user = primary_user_ref.document(str(id)).get().to_dict()
    max_id = -1
    min_id = -1
    direction = None
    end = False
    if "tweets" in user:
        max_id = int(user["tweets"]["max_id"])
        min_id = int(user["tweets"]["min_id"])
        if "direction" in user["tweets"]:
            direction = user["tweets"]["direction"]
        if "end" in user["tweets"]:
            end = user["tweets"]["end"]

    # if this is a new user, start with the first page
    firsterror = False
    if max_id == -1 and min_id == -1:
        logger.info(' - '.join(['DOWNLOADING TIMELINE', 'first', str(id)]))
        try:
            tweets = api.user_timeline(user_id=id, page=1, count=200, tweet_mode="extended")
        except tweepy.TweepError as e:
            tweets = None
            logger.error(e)
            logger.error(' - '.join(['COULD NOT DOWNLOAD TIMELINE', 'first', str(id)]))
            try:
                if e.api_code != 88 and e.api_code != 32:
                    firsterror = True
            except:
                pass
            pass
    # traverse to get older tweets if they haven't yet been exhausted
    elif direction == "older":
        logger.info(' - '.join(['DOWNLOADING TIMELINE', 'max_id', str(id), str(min_id)]))
        try:
            tweets = api.user_timeline(user_id=id, max_id=min_id, count=200, tweet_mode="extended")
        except tweepy.TweepError as e:
            tweets = None
            logger.error(e)
            logger.info(' - '.join(['COULD NOT DOWNLOAD TIMELINE', 'max_id', str(id), str(min_id)]))
            pass
    # otherwise, use the max_id to get newer tweets
    else:
        logger.info(' - '.join(['DOWNLOADING TIMELINE', 'since_id', str(id), str(max_id)]))
        try:
            tweets = api.user_timeline(user_id=id, since_id=max_id, count=200, tweet_mode="extended")
        except tweepy.TweepError as e:
            tweets = None
            logger.error(e)
            logger.info(' - '.join(['COULD NOT DOWNLOAD TIMELINE', 'since_id', str(id), str(max_id)]))
            pass

    # parse tweets
    if tweets:
        actions = []
        if direction == "older" and len(tweets) == 1:
            end = True
        for tweet in tweets:

            # set min and max tweet ids
            if max_id == -1:
                max_id = tweet.id
            if min_id == -1:
                min_id = tweet.id
            max_id = max(max_id, tweet.id)
            min_id = min(min_id, tweet.id)

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

    if direction == "older" or end is True:
        direction = "newer"
    else:
        direction = "older"

    # update the user record in Firestore
    record = {
        "tweets": {
            "min_id": str(min_id),
            "max_id": str(max_id),
            "direction": direction,
            "end": end
        },
        "last_get_timeline": datetime.datetime.now(datetime.timezone.utc),
        "last_updated": datetime.datetime.now(datetime.timezone.utc)
    }
    if firsterror:
        primary_user_ref.document(str(id)).set({
            "last_error": firestore.ArrayUnion([datetime.datetime.now(datetime.timezone.utc)]),
            "last_updated": datetime.datetime.now(datetime.timezone.utc)
        }, merge=True)
        logger.error(' - '.join(['RECORDED USER ERROR', str(id)]))
    else:
        primary_user_ref.document(str(id)).set(record, merge=True)
    logger.info(' - '.join(['USER META UPDATED', str(id)]))

    # return id
    return id
