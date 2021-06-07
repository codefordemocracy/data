import logging
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from google.cloud import secretmanager
from google.cloud import pubsub
from operator import itemgetter
import datetime

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

def news_articles_ingest_queue_duplicates(message, context):

    # set time to begin looking for duplicates
    start = datetime.datetime.now() - datetime.timedelta(days=1)

    # scan through the documents
    s = Search(using=es, index='news_articles').source(['extracted.url','meta.last_indexed'])
    docs = s.filter('range', meta__last_indexed={"gt": start}).scan()

    # for each URL, create an array of its documents
    urls = dict()
    for doc in docs:
        id = doc.meta.id
        doc = doc.to_dict()
        urls.setdefault(doc["extracted"]["url"], []).append({"id": id, "last_indexed": doc["meta"]["last_indexed"]})
    logger.info(' - '.join(['INFO', 'dictionary of urls created', str(len(urls))]))

    # add duplicate articles to a queue for deletion
    queue = []
    for url, docs in urls.items():
        if len(docs) > 1:
            all = sorted(docs, key=itemgetter('last_indexed'))
            keep = all[0]
            delete = all[1:]
            for doc in delete:
                queue.append(doc['id'])
    logger.info(' - '.join(['ARTICLES FOUND FOR DELETION', str(len(queue))]))

    if len(queue) > 0:
        for id in queue:
            # send a message to Pub/Sub telling delete_duplicate to delete the document
            topic = 'projects/' + gcp_project_id + '/topics/news_articles_ingest_delete_duplicate'
            publisher.publish(topic, b'delete this document', id=id)
            logger.info(' - '.join(['INFO', 'document sent to news_articles_ingest_delete_duplicate queue', id]))
        logger.info(' - '.join(['COMPLETED', 'articles sent for deletion', str(len(queue))]))
    else:
        logger.info(' - '.join(['COMPLETED', 'no duplicates found']))

    # return number of articles for deletion
    return len(queue)
