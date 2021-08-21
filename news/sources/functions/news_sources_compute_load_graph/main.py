import logging
import json
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
from neo4j import GraphDatabase
import cypher

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
neo4j_connection = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_connection/versions/1"}).payload.data.decode()
neo4j_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_username_data/versions/1"}).payload.data.decode()
neo4j_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_password_data/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
driver = GraphDatabase.driver(neo4j_connection, auth=(neo4j_username_data, neo4j_password_data))

# utility function to divide rows into batches of n
def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]

# loads source into Neo4j
def news_sources_compute_load_graph(message, context):

    # get documents
    s = Search(using=es, index="news_sources")
    docs = s.scan()

    # parse the sources
    sources = []
    for doc in docs:
        doc = doc.to_dict()
        # calculate the average bias score
        num_ratings = 0
        total_score = 0.0
        average_score = None
        if doc["extracted"].get("mediabiasfactcheck", {}).get("bias_score") is not None:
            num_ratings += 1
            total_score += doc["extracted"].get("mediabiasfactcheck", {}).get("bias_score")
        if doc["extracted"].get("allsides", {}).get("bias_score") is not None:
            num_ratings += 1
            total_score += doc["extracted"].get("allsides", {}).get("bias_score")
        if num_ratings > 1:
            average_score = total_score/num_ratings
        elif num_ratings == 1:
            average_score = total_score
        # calculate the flags
        factually_questionable_flag = None
        if doc["extracted"].get("mediabiasfactcheck", {}).get("factually_questionable_flag") is not None:
            factually_questionable_flag = int(doc["extracted"].get("mediabiasfactcheck", {}).get("factually_questionable_flag"))
        conspiracy_flag = None
        if doc["extracted"].get("mediabiasfactcheck", {}).get("conspiracy_flag") is not None:
            conspiracy_flag = int(doc["extracted"].get("mediabiasfactcheck", {}).get("conspiracy_flag"))
        satire_flag = None
        if doc["extracted"].get("mediabiasfactcheck", {}).get("satire_flag") is not None:
            satire_flag = int(doc["extracted"].get("mediabiasfactcheck", {}).get("satire_flag"))
        hate_group_flag = None
        if doc["extracted"].get("mediabiasfactcheck", {}).get("hate_group_flag") is not None:
            hate_group_flag = int(doc["extracted"].get("mediabiasfactcheck", {}).get("hate_group_flag"))
        propaganda_flag = None
        if doc["extracted"].get("mediabiasfactcheck", {}).get("propaganda_flag") is not None:
            propaganda_flag = int(doc["extracted"].get("mediabiasfactcheck", {}).get("propaganda_flag"))
        # create the batch update
        sources.append({
            "domain": doc["extracted"]["domain"],
            "name": doc["extracted"]["name"].upper(),
            "bias_score": average_score,
            "factually_questionable_flag": factually_questionable_flag,
            "conspiracy_flag": conspiracy_flag,
            "satire_flag": satire_flag,
            "hate_group_flag": hate_group_flag,
            "propaganda_flag": propaganda_flag
        })

    # load into the graph in batches of 1000
    with driver.session() as neo4j:
        for chunk in chunks(sources, 1000):
            neo4j.write_transaction(cypher.merge_node_source, batch=chunk)
            logger.info(' - '.join(['INFO', 'sources loaded', str(len(chunk))]))

    logger.info(' - '.join(['COMPLETED', 'loaded domains', str(len(sources))]))

    return len(sources)
