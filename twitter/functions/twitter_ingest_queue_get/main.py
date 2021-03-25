import logging
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import pubsub
from azure.cosmosdb.table.models import Entity
from azure.cosmosdb.table.tableservice import TableService
import datetime
import json

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
azure_account_name_twitter = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/azure_account_name_twitter/versions/1"}).payload.data.decode()
azure_account_key_twitter = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/azure_account_key_twitter/versions/1"}).payload.data.decode()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
table_service = TableService(account_name=azure_account_name_twitter, account_key=azure_account_key_twitter)
db = firestore.Client()
publisher = pubsub.PublisherClient()

# utility function to divide rows into batches of n
def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]

# queue all Twitter get functions in Pub/Sub
def twitter_ingest_queue_get(message, context):

    # twitter_ingest_primary_get_users
    # grab 30000 user ids from Firestore and deliver to Pub/Sub in batches of 100
    # Twitter API limit is 300 calls per 15 mins
    ref = db.collection('twitter').document('users').collection('primary')
    docs = ref.order_by('last_updated').limit(100).select('id').stream()
    ids = []
    for doc in docs:
        ids.append(int(doc.id))
    for chunk in chunks(ids, 50):
        # send to Pub/Sub
        chunk1 = chunk[::2]
        chunk2 = chunk[1::2]
        topic = 'projects/' + gcp_project_id + '/topics/twitter_ingest_primary_get_users'
        publisher.publish(topic, b'get twitter users', chunk1=json.dumps(chunk1), chunk2=json.dumps(chunk2))
        logger.info(' - '.join(['STATUS', 'chunk sent to twitter_ingest_primary_get_users queue', str(len(chunk))]))
    logger.info(' - '.join(['QUEUED', 'twitter_ingest_primary_get_users', str(len(ids))]))

    # twitter_ingest_primary_get_timeline
    # grab 1388 user ids from Firestore and deliver to Pub/Sub to retrieve 200 tweets for each user
    # Twitter API limit is 1500 calls per 15 mins and 100,000 per day
    ref = db.collection('twitter').document('users').collection('primary')
    yesteryear = datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=365)
    docs = ref.where("obj.protected","==",False).where('last_updated','>',yesteryear).order_by('last_updated').limit(1388).select('id').stream()
    ids = []
    for doc in docs:
        ids.append(doc.id)
    for id in ids:
        # send to Pub/Sub
        topic = 'projects/' + gcp_project_id + '/topics/twitter_ingest_primary_get_timeline'
        publisher.publish(topic, b'get twitter timeline', id=id)
        logger.info(' - '.join(['STATUS', 'id sent to twitter_ingest_primary_get_timeline queue', id]))
    logger.info(' - '.join(['QUEUED', 'twitter_ingest_primary_get_timeline',str(len(ids))]))

    # twitter_ingest_primary_get_tweets
    # grab up to 30000 user ids from Firestore and deliver to Pub/Sub in batches of 100
    # Twitter API limit is 300 calls per 15 mins
    ref = db.collection('twitter').document('tweets').collection('primary')
    yesteryear = datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=365)
    docs = ref.where('last_updated','>',yesteryear).order_by('last_updated').limit(30000).select('id').stream()
    ids = []
    for doc in docs:
        ids.append(int(doc.id))
    for chunk in chunks(ids, 75):
        # send to Pub/Sub
        chunk1 = chunk[::3]
        chunk2 = chunk[1::3]
        chunk3 = chunk[2::3]
        topic = 'projects/' + gcp_project_id + '/topics/twitter_ingest_primary_get_tweets'
        publisher.publish(topic, b'get twitter tweets', chunk1=json.dumps(chunk1), chunk2=json.dumps(chunk2), chunk3=json.dumps(chunk3))
        logger.info(' - '.join(['STATUS', 'chunk sent to twitter_ingest_primary_get_tweets queue', str(len(chunk))]))
    logger.info(' - '.join(['QUEUED', 'twitter_ingest_primary_get_tweets', str(len(ids))]))

    # twitter_ingest_secondary_get_users
    # grab 30000 user ids from Firestore and deliver to Pub/Sub in batches of 100
    # Twitter API limit is 300 calls per 15 mins
    ref = db.collection('twitter').document('users').collection('secondary')
    docs = ref.order_by('last_updated').limit(10000).select('id').stream()
    ids = []
    for doc in docs:
        ids.append(int(doc.id))
    for chunk in chunks(ids, 50):
        # send to Pub/Sub
        chunk1 = chunk[::2]
        chunk2 = chunk[1::2]
        topic = 'projects/' + gcp_project_id + '/topics/twitter_ingest_secondary_get_users'
        publisher.publish(topic, b'get twitter users', chunk1=json.dumps(chunk1), chunk2=json.dumps(chunk2))
        logger.info(' - '.join(['STATUS', 'chunk sent to twitter_ingest_secondary_get_users queue', str(len(chunk))]))
    logger.info(' - '.join(['QUEUED', 'twitter_ingest_secondary_get_users', str(len(ids))]))

    # delete secondary users with many errors from firestore and classify them as error in table storage
    ref = db.collection('twitter').document('users').collection('secondary')
    docs = ref.order_by("last_error").order_by("last_updated", direction=firestore.Query.DESCENDING).limit(1000).stream()
    deleted = 0
    for doc in docs:
        user = doc.to_dict()
        if "last_error" in user:
            if len(user["last_error"]) > 20:
                if "obj" in user:
                    row = Entity()
                    row.PartitionKey = user["obj"]["id"]
                    row.RowKey = user["obj"]["id"]
                    row.classification = "error"
                    row.last_classified = datetime.datetime.now(datetime.timezone.utc)
                    table_service.merge_entity('users', row)
                ref.document(doc.id).delete()
                deleted += 1
                logger.info(' - '.join(['STATUS', 'deleted secondary user with many errors', doc.id]))
    logger.info(' - '.join(['COMPLETED', 'deleted secondary users with errors', str(deleted)]))

    return True
