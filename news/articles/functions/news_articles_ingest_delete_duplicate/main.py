import logging
from google.cloud import secretmanager
from elasticsearch import Elasticsearch
from google.cloud import firestore

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

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
db = firestore.Client()

# deletes each duplicate document from ElasticSearch and Firestore
def news_articles_ingest_delete_duplicate(message, context):

    # set up Firestore refs
    ref = db.collection('news').document('articles').collection('scraped')

    # get the document id from the Pub/Sub message
    id = message['attributes']['id']

    # delete the document from ElasicSearch
    try:
        es.delete(index='news_articles',id=id)
        logger.info(' - '.join(['DELETED FROM ELASTICSEARCH', id]))
    except Exception as e:
        logger.error(' - '.join(['FAILED TO DELETE FROM ELASTICSEARCH', id, str(e)]))

    # delete the document from Firestore
    try:
        ref.document(id).delete()
        logger.info(' - '.join(['DOCUMENT NO LONGER EXISTS IN FIRESTORE', id]))
    except Exception as e:
        logger.error(' - '.join(['FAILED TO DELETE FROM FIRESTORE', id, str(e)]))

    # return the id
    return id
