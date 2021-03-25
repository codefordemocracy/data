import logging
from google.cloud import secretmanager
from google.cloud import firestore
from elasticsearch import Elasticsearch, helpers
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity
import tweepy
import datetime
import json

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

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
table_service = TableService(account_name=azure_account_name_twitter, account_key=azure_account_key_twitter)
db = firestore.Client()

# authenticate Twitter API
auth = tweepy.OAuthHandler(twitter_client_id, twitter_client_secret)
api = tweepy.API(auth, retry_count=1)

# get hydrated user information of a list of user ids and update Firestore, Elasticsearch, and Table Storage
def twitter_ingest_primary_get_users(message, context):

    # get the chunk of ids from the Pub/Sub message
    chunk1 = json.loads(message['attributes']['chunk1'])
    chunk2 = json.loads(message['attributes']['chunk2'])
    chunk = chunk1 + chunk2

    # set up Firestore refs
    ref = db.collection('twitter').document('users').collection('primary')

    # get user objects from Twitter API and update the Firestore user object
    try:
        users = api.lookup_users(user_ids=chunk)
    except tweepy.TweepError as e:
        logger.error(' - '.join(['ERROR WHEN HYDRATING USERS', str(e)]))
        try:
            if e.api_code == 17:
                users = None
                pass
        except:
            raise

    found = []
    if users:
        actions = []
        for user in users:

            # add the user to the found list
            found.append(user.id)

            # process the user
            record = {
                "obj": {
                    "id": user.id_str,
                    "name": user.name,
                    "screen_name": user.screen_name,
                    "location": user.location,
                    "url": user.url,
                    "description": user.description,
                    "protected": user.protected,
                    "verified": user.verified,
                    "followers_count": user.followers_count,
                    "friends_count": user.friends_count,
                    "listed_count": user.listed_count,
                    "favorites_count": user.favourites_count,
                    "statuses_count": user.statuses_count,
                    "created_at": user.created_at,
                    "profile_image_url": user.profile_image_url_https,
                    "default_profile_image": user.default_profile_image
                },
                "classification": "primary",
                "hydrated": True,
                "last_hydrated": datetime.datetime.now(datetime.timezone.utc),
                "last_updated": datetime.datetime.now(datetime.timezone.utc)
            }
            # update ElasticSearch
            actions.append({
                "_op_type": "index",
                "_index": "twitter_users",
                "_id": record["obj"]["id"],
                "_source": record
            })
            # update Table Storage
            row = Entity()
            row.PartitionKey = record["obj"]["id"]
            row.RowKey = record["obj"]["id"]
            row.name = record["obj"]["name"]
            row.screen_name = record["obj"]["screen_name"]
            row.location = record["obj"]["location"]
            row.url = record["obj"]["url"]
            row.description = record["obj"]["description"]
            row.protected = record["obj"]["protected"]
            row.verified = record["obj"]["verified"]
            row.followers_count = record["obj"]["followers_count"]
            row.friends_count = record["obj"]["friends_count"]
            row.listed_count = record["obj"]["listed_count"]
            row.favorites_count = record["obj"]["favorites_count"]
            row.statuses_count = record["obj"]["statuses_count"]
            row.created_at = record["obj"]["created_at"]
            row.profile_image_url = record["obj"]["profile_image_url"]
            row.default_profile_image = record["obj"]["default_profile_image"]
            row.classification = record["classification"]
            row.hydrated = record["hydrated"]
            row.last_hydrated = record["last_hydrated"]
            table_service.insert_or_merge_entity('users', row)
            logger.info(' - '.join(['USER UPDATED IN TABLE STORAGE', user.id_str]))
            # update Firestore
            ref.document(user.id_str).set(record, merge=True)
            logger.info(' - '.join(['USER UPDATED IN FIRESTORE', user.id_str]))

        # bulk update elasticsearch
        helpers.bulk(es, actions)
        logger.info(' - '.join(['TWEETERS SYNCED TO ELASTICSEARCH', str(len(actions))]))

    else:
        logger.error(' - '.join(['NO USERS RETURNED FROM TWITTER API']))

    # log error for users who weren't found
    chunk_set = set(chunk)
    found_set = set(found)
    error_ids = list(chunk_set.difference(found_set))
    for id in error_ids:
        ref.document(str(id)).set({
            "last_error": firestore.ArrayUnion([datetime.datetime.now(datetime.timezone.utc)]),
            "last_updated": datetime.datetime.now(datetime.timezone.utc)
        }, merge=True)
        logger.info(' - '.join(['MISSING USER RECORDED', str(id)]))

    # return number of processed users
    return len(chunk)
