import logging
from google.cloud import secretmanager
from google.cloud import bigquery
from elasticsearch import Elasticsearch, helpers
import datetime
import pandas
import numpy as np

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

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
client = bigquery.Client()

# helper function to generate a new job config
def gen_job_config():
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    return job_config

# load linkages from BigQuery into Elasticsearch
def federal_fec_compute_load_elastic_linkages(message, context):

    query_job = client.query("""
    SELECT cmte_id, cand_id, cand_election_yr, linkage_id
    FROM `federal_fec.ccl22`
    """, job_config=gen_job_config())
    df = query_job.result().to_dataframe()
    df = df.replace({np.nan: None})
    assert query_job.state == "DONE"

    candidates_linkages = dict()
    committees_linkages = dict()
    actions = []
    for index, row in df.iterrows():
        if row["cand_id"] not in candidates_linkages:
            candidates_linkages[row["cand_id"]] = dict()
        candidates_linkages[row["cand_id"]][row["linkage_id"]] = {
            "cmte_id": row["cmte_id"],
            "cand_election_yr": row["cand_election_yr"],
            "linkage_id": row["linkage_id"]
        }
        if row["cmte_id"] not in committees_linkages:
            committees_linkages[row["cmte_id"]] = dict()
        committees_linkages[row["cmte_id"]][row["linkage_id"]] = {
            "cand_id": row["cand_id"],
            "cand_election_yr": row["cand_election_yr"],
            "linkage_id": row["linkage_id"]
        }
    for key in committees_linkages.keys():
        candidates_array = []
        for linkage in committees_linkages[key].keys():
            candidates_array.append(committees_linkages[key][linkage])
        actions.append({
            "_op_type": "update",
            "_index": "federal_fec_committees",
            "_id": key,
            "doc": {
                "linkages": {
                    "candidates": candidates_array
                },
                "context": {
                    "last_linked": datetime.datetime.now(datetime.timezone.utc)
                }
            },
            "doc_as_upsert": True,
            "retry_on_conflict": 3
        })
    for key in candidates_linkages.keys():
        commitees_array = []
        for linkage in candidates_linkages[key].keys():
            commitees_array.append(candidates_linkages[key][linkage])
        actions.append({
            "_op_type": "update",
            "_index": "federal_fec_candidates",
            "_id": key,
            "doc": {
                "linkages": {
                    "committees": commitees_array
                },
                "context": {
                    "last_linked": datetime.datetime.now(datetime.timezone.utc)
                }
            },
            "doc_as_upsert": True,
            "retry_on_conflict": 3
        })

    if len(actions) > 0:
        helpers.bulk(es, actions)
        logger.info(' - '.join(['LOADED LINKAGES INTO ELASTICSEARCH', str(len(actions))]))

    return len(actions)
