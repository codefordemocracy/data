import logging
from google.cloud import secretmanager
from neo4j import GraphDatabase
import time

import cypher

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
neo4j_connection = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_connection/versions/1"}).payload.data.decode()
neo4j_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_username_data/versions/1"}).payload.data.decode()
neo4j_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_password_data/versions/1"}).payload.data.decode()

# connect to resources
driver = GraphDatabase.driver(neo4j_connection, auth=(neo4j_username_data, neo4j_password_data))

# utility function to get domain
def get_domain(url):
    host = url.split("/")[0]
    if host.startswith("www."):
        host = host.replace("www.", "", 1)
    return host

# extracts domains from links
def twitter_compute_extract_domains(message, context):

    # count total
    total = 0

    # get start time
    start = time.time()

    # loop for 520s
    while time.time()-start < 520:

        # get links without domains from neo4j
        urls = []
        with driver.session() as neo4j:
            links = neo4j.read_transaction(cypher.get_links)
            for link in links:
                urls.append({
                    "url": link["url"],
                    "domain": get_domain(link["url"])
                })
            if len(urls) > 0:
                neo4j.write_transaction(cypher.merge_link_domain, batch=urls)
                total += len(urls)
                logger.info(' - '.join(['LINKS MERGED WITH DOMAINS', str(len(urls))]))
            else:
                break

    return total
