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

# send domains that errored out to get_articles again
def news_articles_ingest_process_stragglers(message, context):

    # set up Firestore refs
    queue_ref = db.collection('news').document('queues').collection('crawler')
    
    # send a message to Pub/Sub telling get_articles to scrape the straggling domains
    size = 0
    docs = queue_ref.select("id").get()
    for doc in docs:
        size += 1
        topic = 'projects/' + gcp_project_id + '/topics/news_articles_ingest_get_articles'
        publisher.publish(topic, b'extract articles from this domain', domain=doc.id)
        logger.info(' - '.join(['COMPLETED', 'domain sent to news_articles_ingest_get_articles queue', doc.id]))

    # display and return number of straggling domains
    logger.info(' - '.join(['COMPLETED', 'straggling domains queued', str(size)]))
    return size
