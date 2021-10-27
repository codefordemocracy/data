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
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme='https', port=443, timeout=60)
driver = GraphDatabase.driver(neo4j_connection, auth=(neo4j_username_data, neo4j_password_data))

# helper function to update a record with the transaction date
def add_date(record, date):
    date = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
    record["year"] = date.year
    record["month"] = date.month
    record["day"] = date.day
    record["hour"] = date.hour
    record["minute"] = date.minute
    return record

# load FEC contributions into graph
def federal_fec_compute_load_graph_contributions(message, context):

    # configure ElasticSearch search
    s = Search(using=es, index="federal_fec_contributions")
    q = s.filter("exists", field="row.source").filter("exists", field="row.target").exclude("exists", field="context.last_graphed")

    # get start time
    start = time.time()

    # loop for 520s
    while time.time()-start < 520:

        docs = q[0:1000].sort("-context.last_indexed").execute()
        if len(docs) == 0:
            logger.info(' - '.join(['NO CONTRIBUTIONS FOUND FOR LOADING']))
            break

        # batches for neo4j and elasticsearch
        committee_rows_with_date = []
        committee_rows_without_date = []
        candidate_rows_with_date = []
        candidate_rows_without_date = []
        donor_ind_rows_with_date = []
        donor_ind_rows_without_date = []
        donor_ind_states = []
        donor_ind_zips = []
        donor_org_rows_with_date = []
        donor_org_rows_without_date = []
        actions = []

        for doc in docs:

            # prepare docs for loading
            if doc.row.source.classification == "committee":
                record = {
                    "source": doc.row["source"]["committee"]["cmte_id"],
                    "target": doc.row["target"]["committee"]["cmte_id"],
                    "transaction_amt": doc.row["transaction_amt"],
                    "amndt_ind": doc.row["amndt_ind"],
                    "rpt_tp": doc.row["rpt_tp"],
                    "transaction_pgi": doc.row["transaction_pgi"],
                    "transaction_tp": doc.row["transaction_tp"],
                    "image_num": doc.row["image_num"],
                    "file_num": doc.row["file_num"],
                    "tran_id": doc.row["tran_id"],
                    "sub_id": doc.row["sub_id"]
                }
                if doc.row["transaction_dt"] is not None:
                    record = add_date(record, doc.row["transaction_dt"])
                    committee_rows_with_date.append(record)
                else:
                    committee_rows_without_date.append(record)
            elif doc.row.source.classification == "candidate":
                record = {
                    "source": doc.row["source"]["candidate"]["cand_id"],
                    "target": doc.row["target"]["committee"]["cmte_id"],
                    "transaction_amt": doc.row["transaction_amt"],
                    "amndt_ind": doc.row["amndt_ind"],
                    "rpt_tp": doc.row["rpt_tp"],
                    "transaction_pgi": doc.row["transaction_pgi"],
                    "transaction_tp": doc.row["transaction_tp"],
                    "image_num": doc.row["image_num"],
                    "file_num": doc.row["file_num"],
                    "tran_id": doc.row["tran_id"],
                    "sub_id": doc.row["sub_id"]
                }
                if doc.row["transaction_dt"] is not None:
                    record = add_date(record, doc.row["transaction_dt"])
                    candidate_rows_with_date.append(record)
                else:
                    candidate_rows_without_date.append(record)
            elif doc.row.source.classification == "individual":
                if doc.row["source"]["donor"]["name"] is not None:
                    record = {
                        "entity_tp": doc.row["source"]["donor"]["entity_tp"],
                        "name": doc.processed["source"]["donor"]["name"].strip() if doc.processed["source"]["donor"]["name"] is not None else "",
                        "state": doc.row["source"]["donor"]["state"] or "",
                        "zip_code": doc.row["source"]["donor"]["zip_code"] or "",
                        "employer": doc.row["source"]["donor"]["employer"].strip() if doc.row["source"]["donor"]["employer"] is not None else "",
                        "occupation": doc.row["source"]["donor"]["occupation"].strip() if doc.row["source"]["donor"]["occupation"] is not None else "",
                        "target": doc.row["target"]["committee"]["cmte_id"],
                        "transaction_amt": doc.row["transaction_amt"],
                        "amndt_ind": doc.row["amndt_ind"],
                        "rpt_tp": doc.row["rpt_tp"],
                        "transaction_pgi": doc.row["transaction_pgi"],
                        "transaction_tp": doc.row["transaction_tp"],
                        "image_num": doc.row["image_num"],
                        "file_num": doc.row["file_num"],
                        "tran_id": doc.row["tran_id"],
                        "sub_id": doc.row["sub_id"]
                    }
                    if doc.row["transaction_dt"] is not None:
                        record = add_date(record, doc.row["transaction_dt"])
                        donor_ind_rows_with_date.append(record)
                    else:
                        donor_ind_rows_without_date.append(record)
                    if doc.row["source"]["donor"]["state"] is not None:
                        donor_ind_states.append({
                            "name": doc.processed["source"]["donor"]["name"].strip() if doc.processed["source"]["donor"]["name"] is not None else "",
                            "zip_code": doc.row["source"]["donor"]["zip_code"],
                            "state": doc.row["source"]["donor"]["state"]
                        })
                    if doc.row["source"]["donor"]["zip_code"] is not None:
                        donor_ind_zips.append({
                            "name": doc.processed["source"]["donor"]["name"].strip() if doc.processed["source"]["donor"]["name"] is not None else "",
                            "zip_code": doc.row["source"]["donor"]["zip_code"]
                        })
            elif doc.row.source.classification == "organization":
                if doc.row["source"]["donor"]["name"] is not None:
                    record = {
                        "entity_tp": doc.row["source"]["donor"]["entity_tp"],
                        "name": doc.row["source"]["donor"]["name"].strip() if doc.row["source"]["donor"]["name"] is not None else "",
                        "state": doc.row["source"]["donor"]["state"] or "",
                        "zip_code": doc.row["source"]["donor"]["zip_code"] or "",
                        "target": doc.row["target"]["committee"]["cmte_id"],
                        "transaction_amt": doc.row["transaction_amt"],
                        "amndt_ind": doc.row["amndt_ind"],
                        "rpt_tp": doc.row["rpt_tp"],
                        "transaction_pgi": doc.row["transaction_pgi"],
                        "transaction_tp": doc.row["transaction_tp"],
                        "image_num": doc.row["image_num"],
                        "file_num": doc.row["file_num"],
                        "tran_id": doc.row["tran_id"],
                        "sub_id": doc.row["sub_id"]
                    }
                    if doc.row["transaction_dt"] is not None:
                        record = add_date(record, doc.row["transaction_dt"])
                        donor_org_rows_with_date.append(record)
                    else:
                        donor_org_rows_without_date.append(record)

            # prepare to mark as in graph in elasticsearch
            actions.append({
                "_op_type": "update",
                "_index": "federal_fec_contributions",
                "_id": doc.meta.id,
                "doc": {
                    "context": {
                        "last_graphed": datetime.datetime.now(datetime.timezone.utc)
                    }
                }
            })

        # load into neo4j
        with driver.session() as neo4j:
            neo4j.write_transaction(cypher.merge_rel_committee_contributed_to_with_date, batch=committee_rows_with_date)
            neo4j.write_transaction(cypher.merge_rel_committee_contributed_to_without_date, batch=committee_rows_without_date)
            neo4j.write_transaction(cypher.merge_rel_candidate_contributed_to_with_date, batch=candidate_rows_with_date)
            neo4j.write_transaction(cypher.merge_rel_candidate_contributed_to_without_date, batch=candidate_rows_without_date)
            neo4j.write_transaction(cypher.merge_rel_donor_ind_contributed_to_with_date, batch=donor_ind_rows_with_date)
            neo4j.write_transaction(cypher.merge_rel_donor_ind_contributed_to_without_date, batch=donor_ind_rows_without_date)
            neo4j.write_transaction(cypher.merge_rel_donor_ind_state, batch=donor_ind_states)
            neo4j.write_transaction(cypher.merge_rel_donor_ind_zip, batch=donor_ind_zips)
            neo4j.write_transaction(cypher.merge_rel_donor_org_contributed_to_with_date, batch=donor_org_rows_with_date)
            neo4j.write_transaction(cypher.merge_rel_donor_org_contributed_to_without_date, batch=donor_org_rows_without_date)

        # mark as graphed in elasticsearch
        helpers.bulk(es, actions)
        logger.info(' - '.join(['CONTRIBUTIONS LOADED', str(len(actions))]))

    return True
