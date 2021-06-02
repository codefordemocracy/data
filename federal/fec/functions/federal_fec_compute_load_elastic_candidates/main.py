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

# load candidates from BigQuery into Elasticsearch
def federal_fec_compute_load_elastic_candidates(message, context):

    query_job = client.query("""
    SELECT cand_id, cand_name, cand_pty_affiliation, cand_election_yr, cand_office_st, cand_office, cand_office_district, cand_ici, cand_status, cand_pcc, cand_st1, cand_st2, cand_city, cand_st, cand_zip
    FROM `federal_fec.cn22`
    """, job_config=gen_job_config())
    df = query_job.result().to_dataframe()
    df = df.replace({np.nan: None})
    assert query_job.state == "DONE"

    actions = []
    for index, row in df.iterrows():
        actions.append({
            "_op_type": "index",
            "_index": "federal_fec_candidates",
            "_id": row["cand_id"],
            "_source": {
                "obj": {
                    "cand_id": row["cand_id"],
                    "cand_name": row["cand_name"],
                    "cand_pty_affiliation": row["cand_pty_affiliation"],
                    "cand_election_yr": row["cand_election_yr"],
                    "cand_office_st": row["cand_office_st"],
                    "cand_office": row["cand_office"],
                    "cand_office_district": row["cand_office_district"],
                    "cand_ici": row["cand_ici"],
                    "cand_status": row["cand_status"],
                    "cand_pcc": row["cand_pcc"],
                    "cand_st1": row["cand_st1"],
                    "cand_st2": row["cand_st2"],
                    "cand_city": row["cand_city"],
                    "cand_st": row["cand_st"],
                    "cand_zip": row["cand_zip"]
                },
                "meta": {
                    "last_indexed": datetime.datetime.now(datetime.timezone.utc)
                }
            }
        })

    if len(actions) > 0:
        helpers.bulk(es, actions)
        logger.info(' - '.join(['LOADED CANDIDATES INTO ELASTICSEARCH', str(len(actions))]))

    return len(actions)
