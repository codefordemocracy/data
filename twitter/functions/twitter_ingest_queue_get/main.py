import logging
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import pubsub
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
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
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
db = firestore.Client()
publisher = pubsub.PublisherClient()

# utility function to divide rows into batches of n
def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]

# triggers the get functions
def twitter_ingest_queue_get(message, context):

    # twitter_ingest_get_timeline
    # grab 1500 user ids from Elasticsearch and deliver to Pub/Sub to retrieve 100 tweets for each user
    # Twitter API limit is 1500 calls per 15 mins
    s = Search(using=es, index="twitter_users_new")
    q = s.filter('term', context__primary=True).source(False)
    docs = q[:1500].sort("context.last_updated").execute()
    ids = []
    for doc in docs:
        ids.append(doc.meta.id)
    for id in ids:
        # send to Pub/Sub
        topic = 'projects/' + gcp_project_id + '/topics/twitter_ingest_get_timeline'
        publisher.publish(topic, b'get twitter timeline', id=id)
        logger.info(' - '.join(['STATUS', 'id sent to twitter_ingest_get_timeline queue', id]))
    logger.info(' - '.join(['QUEUED', 'twitter_ingest_get_timeline',str(len(ids))]))

    # twitter_ingest_get_tweets
    # grab up to 30000 tweet ids from Firestore and deliver to Pub/Sub in batches of 100
    # Twitter API limit is 300 calls per 15 mins
    ref = db.collection('twitter').document('queues').collection('tweet')
    yesteryear = datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=365)
    docs = ref.where('last_added','>',yesteryear).order_by('last_added').limit(30000).select('id').stream()
    ids = []
    for doc in docs:
        ids.append(int(doc.id))
    for chunk in chunks(ids, 100):
        # send to Pub/Sub
        chunk1 = chunk[::3]
        chunk2 = chunk[1::3]
        chunk3 = chunk[2::3]
        topic = 'projects/' + gcp_project_id + '/topics/twitter_ingest_get_tweets'
        publisher.publish(topic, b'get twitter tweets', chunk1=json.dumps(chunk1), chunk2=json.dumps(chunk2), chunk3=json.dumps(chunk3))
        logger.info(' - '.join(['STATUS', 'chunk sent to twitter_ingest_get_tweets queue', str(len(chunk))]))
    logger.info(' - '.join(['QUEUED', 'twitter_ingest_get_tweets', str(len(ids))]))

    return True
