import logging
from google.cloud import secretmanager
from google.cloud import pubsub
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

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
publisher = pubsub.PublisherClient()

# queues list of active domains from Elasticsearch
def news_articles_ingest_queue_domains(message, context):

    # get domains from elasticsearch
    s = Search(using=es, index="news_sources")
    q = s.filter("exists", field="extracted.host").source(False)
    docs = q[:10000].execute()

    # send the domains to Pub/Sub
    size = 0
    for doc in docs:
        size += 1
        # send a message to Pub/Sub with the domain
        topic = 'projects/' + gcp_project_id + '/topics/news_articles_ingest_get_paper'
        publisher.publish(topic, b'extract this domain', domain=doc.meta.id)
        logger.info(' - '.join(['STATUS', 'domain sent to news_articles_ingest_get_paper queue', doc.meta.id]))

    # display and return number of domains in query
    logger.info(' - '.join(['COMPLETED', 'active domains queued', str(size)]))
    return size
