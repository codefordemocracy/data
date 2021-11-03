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

# load FEC candidates into graph
def federal_fec_compute_load_graph_candidates(message, context):

    # configure ElasticSearch search
    s = Search(using=es, index="federal_fec_candidates")
    q = s.exclude("exists", field="context.last_graphed")

    # get start time
    start = time.time()

    # loop for 520s
    while time.time()-start < 520:

        docs = q[0:1000].execute()
        if len(docs) == 0:
            logger.info(' - '.join(['NO CANDIDATES FOUND FOR LOADING']))
            break

        # batches for neo4j and elasticsearch
        candidates = []
        parties = []
        races = []
        linkages = []
        actions = []

        for doc in docs:

            # prepare docs for loading
            if "row" in doc:
                candidates.append({
                    "cand_id": doc.row["cand_id"],
                    "cand_name": doc.processed["cand_name"].strip() if doc.processed["cand_name"] is not None else "",
                    "cand_pty_affiliation": doc.row["cand_pty_affiliation"],
                    "cand_election_yr": doc.row["cand_election_yr"],
                    "cand_office_st": doc.row["cand_office_st"],
                    "cand_office": doc.row["cand_office"],
                    "cand_office_district": doc.row["cand_office_district"],
                    "cand_ici": doc.row["cand_ici"]
                })
                if doc.row["cand_pty_affiliation"] is not None:
                    parties.append({
                        "cand_id": doc.row["cand_id"],
                        "cand_pty_affiliation": doc.row["cand_pty_affiliation"]
                    })
                races.append({
                    "cand_id": doc.row["cand_id"],
                    "cand_election_yr": doc.row["cand_election_yr"] or "",
                    "cand_office_st": doc.row["cand_office_st"] or "",
                    "cand_office": doc.row["cand_office"] or "",
                    "cand_office_district": doc.row["cand_office_district"] or ""
                })
            if "linkages" in doc:
                if "committees" in doc.linkages:
                    for linkage in doc.linkages.committees:
                        linkages.append({
                            "cmte_id": linkage["cmte_id"],
                            "cand_id": doc.meta.id,
                            "cand_election_yr": linkage["cand_election_yr"],
                            "linkage_id": linkage["linkage_id"]
                        })

            # prepare to mark as in graph in elasticsearch
            actions.append({
                "_op_type": "update",
                "_index": "federal_fec_candidates",
                "_id": doc.meta.id,
                "doc": {
                    "context": {
                        "last_graphed": datetime.datetime.now(datetime.timezone.utc)
                    }
                }
            })

        # load into neo4j
        with driver.session() as neo4j:
            neo4j.write_transaction(cypher.merge_node_candidate, batch=candidates)
            neo4j.write_transaction(cypher.merge_rel_candidate_party, batch=parties)
            neo4j.write_transaction(cypher.merge_rel_candidate_race, batch=races)
            neo4j.write_transaction(cypher.merge_rel_candidate_committee, batch=linkages)

        # mark as graphed in elasticsearch
        helpers.bulk(es, actions)
        logger.info(' - '.join(['CANDIDATES LOADED', str(len(actions))]))

    return True
