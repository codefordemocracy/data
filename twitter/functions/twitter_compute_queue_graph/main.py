import logging
from google.cloud import secretmanager
from google.cloud import firestore
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
from google.cloud import pubsub
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

# queue loading tweets from primary users and the loading queue
def twitter_compute_queue_graph(message, context):

    # queue tweets in queue
    ref = db.collection('twitter').document('queues').collection('graph')
    docs = ref.limit(15000).get()
    ids = []
    for doc in docs:
        ids.append(doc.id)
    for chunk in chunks(ids, 30):
        topic = 'projects/' + gcp_project_id + '/topics/twitter_compute_load_graph'
        publisher.publish(topic, b'load tweets into graph', tweet_ids=json.dumps(chunk))
        logger.info(' - '.join(['STATUS', 'tweet_ids sent to twitter_compute_load_graph queue', str(len(chunk))]))

    # queue primary users
    s = Search(using=es, index="twitter_users_new")
    q = s.filter("term", context__primary=True).source(False)
    docs = q[0:500].sort("-context.last_graphed").execute()
    for doc in docs:
        # send to queue_tweets by user_id
        topic = 'projects/' + gcp_project_id + '/topics/twitter_compute_load_graph'
        publisher.publish(topic, b'load tweets into graph', user_id=doc.meta.id)
        logger.info(' - '.join(['STATUS', 'user_id sent to twitter_compute_load_graph queue', doc.meta.id]))

    return True
