import logging
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import pubsub

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

# takes the list of domains from Cloud Firestore and queues the active ones in Pub/Sub
def news_articles_ingest_queue_domains(message, context):

    # set up Firestore refs
    ref = db.collection('news').document('sources').collection('scraped')

    # search Firestore for domains with hosts
    docs = ref.where('host','>','').get()

    # send the domains to Pub/Sub
    size = 0
    for doc in docs:
        size += 1
        # send a message to Pub/Sub with the domain
        topic = 'projects/' + gcp_project_id + '/topics/news_articles_ingest_get_paper'
        publisher.publish(topic, b'extract this domain', domain=doc.id)
        logger.info(' - '.join(['STATUS', 'domain sent to news_articles_ingest_get_paper queue', doc.id]))

    # display and return number of domains in query
    logger.info(' - '.join(['COMPLETED', 'active domains queued', str(size)]))
    return size
