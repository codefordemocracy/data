import logging
from google.cloud import secretmanager
from google.cloud import bigquery
from elasticsearch import Elasticsearch, helpers
import pytz
import datetime
import pandas
import numpy as np
import time
import random
import json

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

# helper function to get values string from array of values
def get_values_string(values):
    values_string = ""
    for value in values:
        values_string += "("
        try:
            values_string += "'" + value + "'"
        except:
            values_string += str(value)
        values_string += "), "
    values_string = values_string[:-2]
    return values_string

# helper function to parse unaware date into datetime object in UTC
def parse_date(date):
    if date is None or date == "":
        return None
    else:
        unaware = datetime.datetime.strptime(date, "%Y-%m-%d")
        tz = pytz.timezone("America/New_York")
        aware = tz.localize(unaware)
        return aware.astimezone(pytz.utc)

# main helper function to help with looping
def loop():

    # randomly select loaded contributions table to get around 1000 inserts per day
    loaded_contributions_tables = [
        "loaded_contributions22_1",
        "loaded_contributions22_2",
        "loaded_contributions22_3",
        "loaded_contributions22_4",
        "loaded_contributions22_5",
        "loaded_contributions22_6"
    ]
    loaded_contributions_table = random.choice(loaded_contributions_tables)

    # count total rows inserted
    count = 0

    query_job = client.query("""
    SELECT a.classification, a.donor_entity_tp, a.donor_name, a.donor_state, a.donor_zip_code, a.donor_employer, a.donor_occupation, a.source, a.source_cand_name, a.source_cand_pty_affiliation, a.source_cand_election_yr, a.source_cand_office_st, a.source_cand_office, a.source_cand_office_district, a.source_cand_ici, a.source_cand_pcc, a.source_cand_zip, a.source_cmte_nm, a.source_cmte_zip, a.source_cmte_dsgn, a.source_cmte_tp, a.source_cmte_pty_affiliation, a.source_cmte_filing_freq, a.source_org_tp, a.source_connected_org_nm, a.target, a.target_cmte_nm, a.target_cmte_zip, a.target_cmte_dsgn, a.target_cmte_tp, a.target_cmte_pty_affiliation, a.target_cmte_filing_freq, a.target_org_tp, a.target_connected_org_nm, a.transaction_dt, a.transaction_amt, a.amndt_ind, a.rpt_tp, a.transaction_pgi, a.transaction_tp, a.image_num, a.file_num, a.tran_id, a.sub_id
    FROM `federal_fec.contributions_elastic22` a
    LEFT JOIN `federal_fec.loaded_contributions22` b
    ON a.sub_id = b.sub_id
    WHERE b.sub_id IS NULL
    LIMIT 1000
    """, job_config=gen_job_config())
    df = query_job.result().to_dataframe()
    df = df.replace({np.nan: None})
    assert query_job.state == "DONE"

    values = []
    actions = []
    for index, row in df.iterrows():
        processed = {
            "source": {
                "classification": row["classification"],
            },
            "target": {
                "committee": {
                    "cmte_id": row["target"],
                    "cmte_nm": row["target_cmte_nm"],
                    "cmte_zip": row["target_cmte_zip"],
                    "cmte_dsgn": row["target_cmte_dsgn"],
                    "cmte_tp": row["target_cmte_tp"],
                    "cmte_pty_affiliation": row["target_cmte_pty_affiliation"],
                    "cmte_filing_freq": row["target_cmte_filing_freq"],
                    "org_tp": row["target_org_tp"],
                    "connected_org_nm": row["target_connected_org_nm"]
                }
            },
            "transaction_dt": parse_date(row["transaction_dt"]),
            "transaction_amt": row["transaction_amt"],
            "amndt_ind": row["amndt_ind"],
            "rpt_tp": row["rpt_tp"],
            "transaction_pgi": row["transaction_pgi"],
            "transaction_tp": row["transaction_tp"],
            "image_num": row["image_num"],
            "file_num": row["file_num"],
            "tran_id": row["tran_id"],
            "sub_id": row["sub_id"]
        }
        if processed["source"]["classification"] == "individual" or processed["source"]["classification"] == "organization":
            processed["source"]["donor"] = {
                "entity_tp": row["donor_entity_tp"],
                "name": row["donor_name"],
                "state": row["donor_state"],
                "zip_code": row["donor_zip_code"],
                "employer": row["donor_employer"],
                "occupation": row["donor_occupation"]
            }
        elif processed["source"]["classification"] == "candidate":
            processed["source"]["candidate"] = {
                "cand_id": row["source"],
                "cand_name": row["source_cand_name"],
                "cand_pty_affiliation": row["source_cand_pty_affiliation"],
                "cand_election_yr": row["source_cand_election_yr"],
                "cand_office_st": row["source_cand_office_st"],
                "cand_office": row["source_cand_office"],
                "cand_office_district": row["source_cand_office_district"],
                "cand_ici": row["source_cand_ici"],
                "cand_pcc": row["source_cand_pcc"],
                "cand_zip": row["source_cand_zip"]
            }
        elif processed["source"]["classification"] == "committee":
            processed["source"]["committee"] = {
                "cmte_id": row["source"],
                "cmte_nm": row["source_cmte_nm"],
                "cmte_zip": row["source_cmte_zip"],
                "cmte_dsgn": row["source_cmte_dsgn"],
                "cmte_tp": row["source_cmte_tp"],
                "cmte_pty_affiliation": row["source_cmte_pty_affiliation"],
                "cmte_filing_freq": row["source_cmte_filing_freq"],
                "org_tp": row["source_org_tp"],
                "connected_org_nm": row["source_connected_org_nm"]
            }
        actions.append({
            "_op_type": "index",
            "_index": "federal_fec_contributions",
            "_id": row["sub_id"],
            "_source": {
                "processed": processed,
                "meta": {
                    "last_indexed": datetime.datetime.now(datetime.timezone.utc)
                }
            }
        })
        values.append(row["sub_id"])

    if len(values) > 0:
        helpers.bulk(es, actions)
        values_string = get_values_string(values)
        query_job = client.query(f"""
        INSERT INTO `federal_fec.{loaded_contributions_table}` (sub_id)
        VALUES %s
        """ % (values_string), job_config=gen_job_config())
        query_job.result()
        assert query_job.state == "DONE"
        count += query_job.num_dml_affected_rows

    return count

# load contributions from BigQuery into Elasticsearch
def federal_fec_compute_load_elastic_contributions(message, context):

    # count total number of rows loaded
    loaded = 0

    # count rows loaded per batch
    count = 0

    # get start time
    start = time.time()

    # loop for 520s
    section = 0
    while time.time()-start < 520:
        count = loop()
        loaded += count
        if count == 0:
            break

    # log progress
    if count == 0:
        logger.info(' - '.join(['FINISHED LOADING ALL CONTRIBUTIONS INTO ELASTICSEARCH', str(loaded)]))
    else:
        logger.info(' - '.join(['LOADED BATCH OF CONTRIBUTIONS INTO ELASTICSEARCH', str(loaded)]))

    return loaded
