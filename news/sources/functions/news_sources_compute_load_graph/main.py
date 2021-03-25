import logging
import json
from google.cloud import secretmanager
from google.cloud import firestore
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
db = firestore.Client()

# utility function to divide rows into batches of n
def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]

# loads source into Neo4j
def news_sources_compute_load_graph(message, context):

    # set up Firestore refs
    ref = db.collection('news').document('sources').collection('scraped')

    # get sources
    docs = ref.limit(5000).stream()

    # parse the sources
    sources = []
    for doc in docs:
        doc = doc.to_dict()
        sources.append({
            "domain": doc["domain"],
            "name": doc["name"].upper(),
            "bias_score": doc["bias_score"],
            "factually_questionable_flag": doc["factually_questionable_flag"],
            "conspiracy_flag": doc["conspiracy_flag"],
            "satire_flag": doc["satire_flag"],
            "hate_group_flag": doc["hate_group_flag"],
            "propaganda_flag": doc["propaganda_flag"]
        })

    # load into the graph in batches of 1000
    with driver.session() as neo4j:
        for chunk in chunks(sources, 1000):
            neo4j.write_transaction(cypher.merge_node_source, batch=chunk)
            logger.info(' - '.join(['INFO', 'sources loaded', str(len(chunk))]))

    logger.info(' - '.join(['COMPLETED', 'loaded domains', str(len(sources))]))

    return len(sources)
