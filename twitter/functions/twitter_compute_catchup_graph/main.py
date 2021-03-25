import logging
from google.cloud import secretmanager
from google.cloud import pubsub
from neo4j import GraphDatabase
import json

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
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
driver = GraphDatabase.driver(neo4j_connection, auth=(neo4j_username_data, neo4j_password_data))
publisher = pubsub.PublisherClient()

# utility function to divide rows into batches of n
def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]

# searches Neo4j for tweets that errored out and sends for reloading
def twitter_compute_catchup_graph(message, context):

    # get ids of tweets without datetime in Neo4j
    ids = []
    with driver.session() as neo4j:
        tweets = neo4j.read_transaction(cypher.get_tweets)
    for tweet in tweets:
        ids.append(tweet["tweet_id"])

    # send to be reloaded
    for chunk in chunks(ids, 30):
        topic = 'projects/' + gcp_project_id + '/topics/twitter_compute_load_graph'
        publisher.publish(topic, b'load tweets into graph', tweet_ids=json.dumps(chunk))
        logger.info(' - '.join(['INFO', 'ids sent to twitter_compute_load_graph queue', json.dumps(chunk)]))

    logger.info(' - '.join(['TWEETS FOUND', str(len(ids))]))
    return len(ids)
