import logging
from google.cloud import secretmanager
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
publisher = pubsub.PublisherClient()

# sends FEC file paths to be imported into BigQuery
def federal_fec_ingest_queue_import(message, context):

    # list of files to import from Google Cloud Storage
    files = [
        "weball20/weball20.txt",
        "cn20/cn.txt",
        "ccl20/ccl.txt",
        "webl20/webl20.txt",
        "cm20/cm.txt",
        "webk20/webk20.txt",
        "indiv20/itcont.txt",
        "pas220/itpas2.txt",
        "oth20/itoth.txt",
        "oppexp20/oppexp.txt",
        "independent_expenditure_2020/independent_expenditure_2020.csv",
        "ElectioneeringComm_2020/ElectioneeringComm_2020.csv",
        "CommunicationCosts_2020/CommunicationCosts_2020.csv"
    ]

    for filepath in files:

        # sends a message to Pub/Sub to import the files
        topic = 'projects/' + gcp_project_id + '/topics/fec_ingest_import_bigquery'
        publisher.publish(topic, b'import FEC file', filepath=filepath)
        logger.info(' - '.join(['COMPLETED', 'file sent to be imported', filepath]))

    # return number of files queued
    return len(files)
