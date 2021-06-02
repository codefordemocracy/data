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

# load committees from BigQuery into Elasticsearch
def federal_fec_compute_load_elastic_committees(message, context):

    query_job = client.query("""
    SELECT cmte_id, cmte_nm, tres_nm, cmte_st1, cmte_st2, cmte_city, cmte_st, cmte_zip, cmte_dsgn, cmte_tp, cmte_pty_affiliation, cmte_filing_freq, org_tp, connected_org_nm, cand_id
    FROM `federal_fec.cm22`
    """, job_config=gen_job_config())
    df = query_job.result().to_dataframe()
    df = df.replace({np.nan: None})
    assert query_job.state == "DONE"

    actions = []
    for index, row in df.iterrows():
        actions.append({
            "_op_type": "index",
            "_index": "federal_fec_committees",
            "_id": row["cmte_id"],
            "_source": {
                "obj": {
                    "cmte_id": row["cmte_id"],
                    "cmte_nm": row["cmte_nm"],
                    "tres_nm": row["tres_nm"],
                    "cmte_st1": row["cmte_st1"],
                    "cmte_st2": row["cmte_st2"],
                    "cmte_city": row["cmte_city"],
                    "cmte_st": row["cmte_st"],
                    "cmte_zip": row["cmte_zip"],
                    "cmte_dsgn": row["cmte_dsgn"],
                    "cmte_tp": row["cmte_tp"],
                    "cmte_pty_affiliation": row["cmte_pty_affiliation"],
                    "cmte_filing_freq": row["cmte_filing_freq"],
                    "org_tp": row["org_tp"],
                    "connected_org_nm": row["connected_org_nm"],
                    "cand_id": ["cand_id"]
                },
                "meta": {
                    "last_indexed": datetime.datetime.now(datetime.timezone.utc)
                }
            }
        })

    if len(actions) > 0:
        helpers.bulk(es, actions)
        logger.info(' - '.join(['LOADED COMMITTEES INTO ELASTICSEARCH', str(len(actions))]))

    return len(actions)
