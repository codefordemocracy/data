import logging
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import pubsub
import datetime

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
db = firestore.Client()
publisher = pubsub.PublisherClient()

# queue loading tweets from primary users
def twitter_compute_queue_users(message, context):

    # set up Firestore refs
    ref = db.collection('twitter').document('users').collection('primary')

    docs = ref.order_by('last_updated', direction=firestore.Query.DESCENDING).limit(500).stream()
    for doc in docs:
        doc = doc.to_dict()
        # send to queue_tweets by user_id
        topic = 'projects/' + gcp_project_id + '/topics/twitter_compute_load_graph'
        publisher.publish(topic, b'load tweets into graph', user_id=doc["obj"]["id"])
        logger.info(' - '.join(['STATUS', 'user_id sent to twitter_compute_load_graph queue', doc["obj"]["id"]]))
        # send to queue_tweets by looking for mentions
        topic = 'projects/' + gcp_project_id + '/topics/twitter_compute_load_graph'
        publisher.publish(topic, b'load tweets into graph', term=doc["obj"]["screen_name"])
        logger.info(' - '.join(['STATUS', 'term sent to twitter_compute_load_graph queue', doc["obj"]["screen_name"]]))

    return True
