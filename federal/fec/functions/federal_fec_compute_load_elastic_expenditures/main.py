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
    loaded_expenditures_tables = [
        "loaded_expenditures22_1",
        "loaded_expenditures22_2",
        "loaded_expenditures22_3"
    ]
    loaded_expenditures_table = random.choice(loaded_expenditures_tables)

    # count total rows inserted
    count = 0

    query_job = client.query("""
    SELECT a.id, a.type, a.cmte_id, a.cmte_nm, a.transaction_dt, a.transaction_amt, a.purpose, a.sup_opp, a.cand_id, a.cand_name, a.cand_pty_affiliation, a.cand_election_yr, a.cand_office_st, a.cand_office, a.cand_office_district, a.category, a.category_desc, a.payee, a.entity_tp, a.state, a.zip_code, a.transaction_pgi, a.amndt_ind, a.image_num, a.file_num, a.tran_id, a.line_num, a.rpt_yr, a.rpt_tp, a.form_tp_cd, a.sched_tp_cd, a.rec_dt, a.prev_file_num, a.back_ref_tran_id, a.sub_id
    FROM `federal_fec.expenditures22` a
    LEFT JOIN `federal_fec.loaded_expenditures22` b
    ON a.id = b.id
    WHERE b.id IS NULL
    LIMIT 1000
    """, job_config=gen_job_config())
    df = query_job.result().to_dataframe()
    df = df.replace({np.nan: None})
    assert query_job.state == "DONE"

    values = []
    actions = []
    for index, row in df.iterrows():
        processed = {
            "type": row["type"],
            "spender": {
                "cmte_id": row["cmte_id"],
                "cmte_nm": row["cmte_nm"]
            },
            "payee": {
                "name": row["payee"],
                "entity_tp": row["entity_tp"],
                "state": row["state"],
                "zip_code": row["zip_code"]
            },
            "transaction_dt": parse_date(row["transaction_dt"]),
            "transaction_amt": row["transaction_amt"],
            "purpose": row["purpose"],
            "category": row["category"],
            "category_desc": row["category_desc"],
            "transaction_pgi": row["transaction_pgi"],
            "amndt_ind": row["amndt_ind"],
            "image_num": row["image_num"],
            "file_num": row["file_num"],
            "tran_id": row["tran_id"],
            "line_num": row["line_num"],
            "rpt_yr": row["rpt_yr"],
            "rpt_tp": row["rpt_tp"],
            "form_tp_cd": row["form_tp_cd"],
            "sched_tp_cd": row["sched_tp_cd"],
            "rec_dt": parse_date(row["rec_dt"]),
            "prev_file_num": row["prev_file_num"],
            "back_ref_tran_id": row["back_ref_tran_id"],
            "sub_id": row["sub_id"]
        }
        if row["sup_opp"] is not None or row["cand_id"] is not None or row["cand_name"] is not None:
            processed["content"] = {
                "sup_opp": row["sup_opp"],
                "cand_id": row["cand_id"],
                "cand_name": row["cand_name"],
                "cand_pty_affiliation": row["cand_pty_affiliation"],
                "cand_election_yr": row["cand_election_yr"],
                "cand_office_st": row["cand_office_st"],
                "cand_office": row["cand_office"],
                "cand_office_district": row["cand_office_district"]
            }
        actions.append({
            "_op_type": "index",
            "_index": "federal_fec_expenditures",
            "_id": row["id"],
            "_source": {
                "processed": processed,
                "meta": {
                    "last_indexed": datetime.datetime.now(datetime.timezone.utc)
                }
            }
        })
        values.append(row["id"])

    if len(values) > 0:
        helpers.bulk(es, actions)
        values_string = get_values_string(values)
        query_job = client.query(f"""
        INSERT INTO `federal_fec.{loaded_expenditures_table}` (id)
        VALUES %s
        """ % (values_string), job_config=gen_job_config())
        query_job.result()
        assert query_job.state == "DONE"
        count += query_job.num_dml_affected_rows

    return count

# load expenditures from BigQuery into Elasticsearch
def federal_fec_compute_load_elastic_expenditures(message, context):

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
        logger.info(' - '.join(['FINISHED LOADING ALL EXPENDITURES INTO ELASTICSEARCH', str(loaded)]))
    else:
        logger.info(' - '.join(['LOADED BATCH OF EXPENDITURES INTO ELASTICSEARCH', str(loaded)]))

    return loaded
