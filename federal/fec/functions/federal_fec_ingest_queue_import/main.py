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
        "weball22/weball22.txt",
        "cn22/cn.txt",
        "ccl22/ccl.txt",
        "webl22/webl22.txt",
        "cm22/cm.txt",
        "webk22/webk22.txt",
        "indiv22/itcont.txt",
        "pas222/itpas2.txt",
        "oth22/itoth.txt",
        "oppexp22/oppexp.txt",
        "independent_expenditure_2022/independent_expenditure_2022.csv",
        "ElectioneeringComm_2022/ElectioneeringComm_2022.csv",
        "CommunicationCosts_2022/CommunicationCosts_2022.csv"
    ]

    for filepath in files:

        # sends a message to Pub/Sub to import the files
        topic = 'projects/' + gcp_project_id + '/topics/federal_fec_ingest_import_bigquery'
        publisher.publish(topic, b'import FEC file', filepath=filepath)
        logger.info(' - '.join(['COMPLETED', 'file sent to be imported', filepath]))

    # return number of files queued
    return len(files)
