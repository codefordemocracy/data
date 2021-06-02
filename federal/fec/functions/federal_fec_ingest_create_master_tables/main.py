import logging
from google.cloud import bigquery

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# connect to resources
client = bigquery.Client()

# creates the candidates, committees, and contributions tables
def federal_fec_ingest_create_master_tables(message, context):

    # set up bigquery
    dataset_ref = client.dataset('federal_fec')
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False

    # create master contributions table
    table_ref = dataset_ref.table("contributions22")
    logger.info(' - '.join(['INFO', 'deleting contributions table']))
    client.delete_table(table_ref, not_found_ok=True)
    logger.info(' - '.join(['INFO', 'creating contributions table']))
    client.create_table(bigquery.Table(table_ref, schema=[
        bigquery.SchemaField("cmte_id", "STRING"),
        bigquery.SchemaField("other_id", "STRING"),
        bigquery.SchemaField("amndt_ind", "STRING"),
        bigquery.SchemaField("rpt_tp", "STRING"),
        bigquery.SchemaField("transaction_pgi", "STRING"),
        bigquery.SchemaField("transaction_tp", "STRING"),
        bigquery.SchemaField("entity_tp", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("zip_code", "STRING"),
        bigquery.SchemaField("employer", "STRING"),
        bigquery.SchemaField("occupation", "STRING"),
        bigquery.SchemaField("transaction_dt", "STRING"),
        bigquery.SchemaField("transaction_amt", "FLOAT"),
        bigquery.SchemaField("memo_text", "STRING"),
        bigquery.SchemaField("image_num", "STRING"),
        bigquery.SchemaField("file_num", "INTEGER"),
        bigquery.SchemaField("tran_id", "STRING"),
        bigquery.SchemaField("sub_id", "INTEGER")
    ]))
    logger.info(' - '.join(['START', 'loading contributions table']))
    job_config.destination = None
    contributions_job = client.query("""
    INSERT INTO `federal_fec.contributions22` (cmte_id, other_id, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, entity_tp, name, state, zip_code, employer, occupation, transaction_dt, transaction_amt, memo_text, image_num, file_num, tran_id, sub_id)
    SELECT DISTINCT cmte_id, other_id, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, entity_tp, name, state, SUBSTR(zip_code, 0, 5) AS zip_code, employer, occupation, CONCAT(SUBSTR(transaction_dt, 5, 4),'-',SUBSTR(transaction_dt, 1, 2),'-',SUBSTR(transaction_dt, 3, 2)) AS transaction_dt, transaction_amt, memo_text, image_num, file_num, tran_id, sub_id
    FROM (
        SELECT cmte_id, other_id, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, entity_tp, name, state, zip_code, employer, occupation, transaction_dt, transaction_amt, memo_text, image_num, file_num, tran_id, sub_id
        FROM `federal_fec.oth22`
        WHERE memo_cd IS NULL
        UNION ALL
        SELECT cmte_id, other_id, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, entity_tp, name, state, zip_code, employer, occupation, transaction_dt, transaction_amt, memo_text, image_num, file_num, tran_id, sub_id
        FROM `federal_fec.indiv22`
        WHERE memo_cd IS NULL
    ) x
    """, job_config=job_config)
    contributions_job.result()
    assert contributions_job.state == "DONE"
    logger.info(' - '.join(['INFO', 'contributions table loaded']))

    # return true
    return True
