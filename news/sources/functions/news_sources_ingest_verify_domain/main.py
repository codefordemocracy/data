import logging
import json
import socket
from google.cloud import secretmanager
from elasticsearch import Elasticsearch
import time
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

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)

# checks domains to see if the DNS resolves and adds to Elasticsearch
def news_sources_ingest_verify_domain(message, context):

    # get the document with the domain from the Pub/Sub message
    doc = json.loads(message['attributes']['doc'])

    # check the domain for host a few times
    for i in range(12):
        try:
            doc["host"] = socket.gethostbyname(doc['domain'])
        except socket.gaierror as e:
            if e.errno == -5 or e.errno == -2:
                # host could not be found
                logger.warning(' - '.join(['COMPLETED', 'host not found', doc['domain']]))
                break
            elif e.errno == -3:
                # temporary issue with finding host so try again in 1 second
                logger.warning(' - '.join(['TEMPORARY FAILURE IN HOST RESOLUTION', 'retrying in 1 second', doc['domain']]))
                time.sleep(1)
                continue
            else:
                logger.error(' - '.join(['UNKNOWN ERROR RESOLVING HOST', doc['domain'], str(e)]))
                raise
        except Exception as e:
            logger.error(' - '.join(['UNKNOWN ERROR RESOLVING HOST', doc['domain'], str(e)]))
            raise

    record = {
        "extracted": doc,
        "context": {
            "last_updated": datetime.datetime.now(datetime.timezone.utc)
        }
    }
    es.index(index="news_sources", id=doc["domain"], body=record)
    logger.info(' - '.join(['COMPLETED', 'updated Elasticsearch document', doc['domain']]))

    return doc
