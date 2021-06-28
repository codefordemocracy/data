import logging
import json
import socket
from google.cloud import firestore
import time
import datetime

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# connect to resources
db = firestore.Client()

# checks domains to see if the DNS resolves and adds to Firestore
def news_sources_ingest_verify_domain(message, context):

    # get the document with the domain from the Pub/Sub message
    doc = json.loads(message['attributes']['doc'])

    # set up Firestore refs
    ref = db.collection('news').document('sources').collection('scraped').document(doc['domain'])

    doc["host"] = ''
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

    doc["last_updated"] = datetime.datetime.now(datetime.timezone.utc)

    # update Cloud Firestore with doc, overwriting attributes or adding new doc
    ref.set(doc, merge=True)
    logger.info(' - '.join(['COMPLETED', 'updated Firestore document', doc['domain']]))

    return doc
