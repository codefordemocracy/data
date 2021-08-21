import logging
from google.cloud import secretmanager
from neo4j import GraphDatabase
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
import datetime
import time

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
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme='https', port=443)
driver = GraphDatabase.driver(neo4j_connection, auth=(neo4j_username_data, neo4j_password_data))

# load FEC committees into graph
def federal_fec_compute_load_graph_committees(message, context):

    # configure ElasticSearch search
    s = Search(using=es, index="federal_fec_committees")
    q = s.exclude("exists", field="context.last_graphed")

    # get start time
    start = time.time()

    # loop for 520s
    while time.time()-start < 520:

        docs = q[0:1000].execute()
        if len(docs) == 0:
            logger.info(' - '.join(['NO COMMITTEES FOUND FOR LOADING']))
            break

        # batches for neo4j and elasticsearch
        committees = []
        parties = []
        employers = []
        linkages = []
        actions = []

        for doc in docs:

            # prepare docs for loading
            if "row" in doc:
                committees.append({
                    "cmte_id": doc.row["cmte_id"],
                    "cmte_nm": doc.row["cmte_nm"],
                    "cmte_dsgn": doc.row["cmte_dsgn"],
                    "cmte_tp": doc.row["cmte_tp"],
                    "cmte_pty_affiliation": doc.row["cmte_pty_affiliation"],
                    "org_tp": doc.row["org_tp"],
                    "connected_org_nm": doc.row["connected_org_nm"]
                })
                if doc.row["cmte_pty_affiliation"] is not None:
                    parties.append({
                        "cmte_id": doc.row["cmte_id"],
                        "cmte_pty_affiliation": doc.row["cmte_pty_affiliation"]
                    })
                if doc.row["connected_org_nm"] is not None:
                    employers.append({
                        "cmte_id": doc.row["cmte_id"],
                        "connected_org_nm": doc.row["connected_org_nm"]
                    })
            if "linkages" in doc:
                if "candidates" in doc.linkages:
                    for linkage in doc.linkages.candidates:
                        linkages.append({
                            "cmte_id": doc.meta.id,
                            "cand_id": linkage["cand_id"],
                            "cand_election_yr": linkage["cand_election_yr"],
                            "linkage_id": linkage["linkage_id"]
                        })

            # prepare to mark as in graph in elasticsearch
            actions.append({
                "_op_type": "update",
                "_index": "federal_fec_committees",
                "_id": doc.meta.id,
                "doc": {
                    "context": {
                        "last_graphed": datetime.datetime.now(datetime.timezone.utc)
                    }
                }
            })

        # load into neo4j
        with driver.session() as neo4j:
            neo4j.write_transaction(cypher.merge_node_committee, batch=committees)
            neo4j.write_transaction(cypher.merge_rel_committee_party, batch=parties)
            neo4j.write_transaction(cypher.merge_rel_committee_employer, batch=employers)
            neo4j.write_transaction(cypher.merge_rel_committee_candidate, batch=linkages)

        # mark as graphed in elasticsearch
        helpers.bulk(es, actions)
        logger.info(' - '.join(['COMMITTEES LOADED', str(len(actions))]))

    return True
