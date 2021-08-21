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

# helper function to update a record with the transaction date
def add_date(record, date):
    date = datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S%z")
    record["year"] = date.year
    record["month"] = date.month
    record["day"] = date.day
    record["hour"] = date.hour
    record["minute"] = date.minute
    return record

# load FEC expenditures into graph
def federal_fec_compute_load_graph_expenditures(message, context):

    # configure ElasticSearch search
    s = Search(using=es, index="federal_fec_expenditures")
    q = s.filter("exists", field="row").exclude("exists", field="context.last_graphed")
    q = q.filter("term", row__type="independent").filter("exists", field="row.content.cand_id")

    # get start time
    start = time.time()

    # loop for 520s
    while time.time()-start < 520:

        docs = q[0:1000].sort("row.rec_dt").execute()
        if len(docs) == 0:
            logger.info(' - '.join(['NO EXPENDITURES FOUND FOR LOADING']))
            break

        # batches for neo4j and elasticsearch
        ind_new_rows_with_date = []
        ind_new_rows_without_date = []
        ind_amend_rows_with_date = []
        ind_amend_rows_without_date = []
        actions = []

        for doc in docs:

            # prepare docs for loading
            if doc.row.type == "independent":
                record = {
                    "cand_id": doc.row["content"]["cand_id"],
                    "cmte_id": doc.row["spender"]["cmte_id"],
                    "transaction_amt": doc.row["transaction_amt"],
                    "sup_opp": doc.row["content"]["sup_opp"],
                    "purpose": doc.row["purpose"].upper().strip() if doc.row["purpose"] is not None else "",
                    "payee": doc.row["payee"]["name"].upper().strip() if doc.row["payee"]["name"] is not None else "",
                    "amndt_ind": doc.row["amndt_ind"],
                    "image_num": doc.row["image_num"],
                    "tran_id": doc.row["tran_id"],
                    "file_num": doc.row["file_num"],
                    "prev_file_num": doc.row["prev_file_num"]
                }
                if doc.row["transaction_dt"] is not None:
                    record = add_date(record, doc.row["transaction_dt"])
                    if doc.row["prev_file_num"] is not None:
                        ind_amend_rows_with_date.append(record)
                    else:
                        ind_new_rows_with_date.append(record)
                else:
                    if doc.row["prev_file_num"] is not None:
                        ind_amend_rows_without_date.append(record)
                    else:
                        ind_new_rows_without_date.append(record)

            # prepare to mark as in graph in elasticsearch
            actions.append({
                "_op_type": "update",
                "_index": "federal_fec_expenditures",
                "_id": doc.meta.id,
                "doc": {
                    "context": {
                        "last_graphed": datetime.datetime.now(datetime.timezone.utc)
                    }
                }
            })

        # load into neo4j
        with driver.session() as neo4j:
            neo4j.write_transaction(cypher.merge_ind_exp_new_with_date, batch=ind_new_rows_with_date)
            neo4j.write_transaction(cypher.merge_ind_exp_new_without_date, batch=ind_new_rows_without_date)
            neo4j.write_transaction(cypher.merge_ind_exp_amend_with_date, batch=ind_amend_rows_with_date)
            neo4j.write_transaction(cypher.merge_ind_exp_amend_without_date, batch=ind_amend_rows_without_date)

        # mark as graphed in elasticsearch
        helpers.bulk(es, actions)
        logger.info(' - '.join(['EXPENDITURES LOADED', str(len(actions))]))

    return True
