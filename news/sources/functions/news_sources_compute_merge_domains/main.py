import logging
from google.cloud import secretmanager
from neo4j import GraphDatabase

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

# connects domains with sources
def news_sources_compute_merge_domains(message, context):

    with driver.session() as neo4j:
        # get sources
        domains = []
        sources = neo4j.read_transaction(cypher.get_sources)
        for source in sources:
            domains.append({
                "domain": source["domain"]
            })
        # get host and domains pairs
        pairs = []
        matches = neo4j.read_transaction(cypher.match_domains, batch=domains)
        for match in matches:
            pairs.append({
                "host": match["host"],
                "domain": match["domain"]
            })
        # connect pairs
        neo4j.write_transaction(cypher.merge_domain_source, batch=pairs)
        logger.info(' - '.join(['DOMAINS MERGED WITH SOURCES', str(len(pairs))]))

    return len(pairs)
