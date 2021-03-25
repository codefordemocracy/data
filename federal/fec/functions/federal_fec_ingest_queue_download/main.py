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

# sends FEC file urls to be downloaded
def federal_fec_ingest_queue_download(message, context):

    # list of files to download from FEC
    files = [
        "https://www.fec.gov/files/bulk-downloads/2020/weball20.zip",
        "https://www.fec.gov/files/bulk-downloads/2020/cn20.zip",
        "https://www.fec.gov/files/bulk-downloads/2020/ccl20.zip",
        "https://www.fec.gov/files/bulk-downloads/2020/webl20.zip",
        "https://www.fec.gov/files/bulk-downloads/2020/cm20.zip",
        "https://www.fec.gov/files/bulk-downloads/2020/webk20.zip",
        "https://www.fec.gov/files/bulk-downloads/2020/indiv20.zip",
        "https://www.fec.gov/files/bulk-downloads/2020/pas220.zip",
        "https://www.fec.gov/files/bulk-downloads/2020/oth20.zip",
        "https://www.fec.gov/files/bulk-downloads/2020/oppexp20.zip",
        "https://www.fec.gov/files/bulk-downloads/2020/independent_expenditure_2020.csv",
        "https://www.fec.gov/files/bulk-downloads/2020/ElectioneeringComm_2020.csv",
        "https://www.fec.gov/files/bulk-downloads/2020/CommunicationCosts_2020.csv"
    ]

    for zipurl in files:

        # sends a message to Pub/Sub to download the files
        topic = 'projects/' + gcp_project_id + '/topics/fec_ingest_download_zip'
        publisher.publish(topic, b'download FEC zip file', zipurl=zipurl)
        logger.info(' - '.join(['COMPLETED', 'file sent to be downloaded', zipurl]))

    # return number of files queued
    return len(files)
