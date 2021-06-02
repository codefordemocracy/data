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

    # create master expenditures table
    table_ref = dataset_ref.table("expenditures22")
    logger.info(' - '.join(['INFO', 'deleting expenditures table']))
    client.delete_table(table_ref, not_found_ok=True)
    logger.info(' - '.join(['INFO', 'creating expenditures table']))
    client.create_table(bigquery.Table(table_ref, schema=[
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("cmte_id", "STRING"),
        bigquery.SchemaField("cmte_nm", "STRING"),
        bigquery.SchemaField("transaction_dt", "STRING"),
        bigquery.SchemaField("transaction_amt", "FLOAT"),
        bigquery.SchemaField("purpose", "STRING"),
        bigquery.SchemaField("sup_opp", "STRING"),
        bigquery.SchemaField("cand_id", "STRING"),
        bigquery.SchemaField("cand_name", "STRING"),
        bigquery.SchemaField("cand_pty_affiliation", "STRING"),
        bigquery.SchemaField("cand_election_yr", "INTEGER"),
        bigquery.SchemaField("cand_office_st", "STRING"),
        bigquery.SchemaField("cand_office", "STRING"),
        bigquery.SchemaField("cand_office_district", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("category_desc", "STRING"),
        bigquery.SchemaField("payee", "STRING"),
        bigquery.SchemaField("entity_tp", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("zip_code", "STRING"),
        bigquery.SchemaField("transaction_pgi", "STRING"),
        bigquery.SchemaField("amndt_ind", "STRING"),
        bigquery.SchemaField("image_num", "STRING"),
        bigquery.SchemaField("file_num", "INTEGER"),
        bigquery.SchemaField("tran_id", "STRING"),
        bigquery.SchemaField("line_num", "STRING"),
        bigquery.SchemaField("rpt_yr", "INTEGER"),
        bigquery.SchemaField("rpt_tp", "STRING"),
        bigquery.SchemaField("form_tp_cd", "STRING"),
        bigquery.SchemaField("sched_tp_cd", "STRING"),
        bigquery.SchemaField("rec_dt", "STRING"),
        bigquery.SchemaField("prev_file_num", "INTEGER"),
        bigquery.SchemaField("back_ref_tran_id", "STRING"),
        bigquery.SchemaField("sub_id", "INTEGER")
    ]))
    logger.info(' - '.join(['START', 'loading expenditures table']))
    job_config.destination = None
    expenditures_job = client.query("""
    INSERT INTO `federal_fec.expenditures22` (id, type, cmte_id, cmte_nm, transaction_dt, transaction_amt, purpose, sup_opp, cand_id, cand_name, cand_pty_affiliation, cand_election_yr, cand_office_st, cand_office, cand_office_district, category, category_desc, payee, entity_tp, state, zip_code, transaction_pgi, amndt_ind, image_num, file_num, tran_id, line_num, rpt_yr, rpt_tp, form_tp_cd, sched_tp_cd, rec_dt, prev_file_num, back_ref_tran_id, sub_id)
    SELECT CAST(a.sub_id AS STRING) AS id, 'operating' AS type, a.cmte_id, b.cmte_nm, CAST(PARSE_DATE('%m/%d/%Y', a.transaction_dt) AS STRING) AS transaction_dt, a.transaction_amt, a.purpose, null AS sup_opp, null AS cand_id, null AS cand_name, null AS cand_pty_affiliation, null AS cand_election_yr, null AS cand_office_st, null AS cand_office, null AS cand_office_district, a.category, a.category_desc, a.name as payee, a.entity_tp, a.state, SUBSTR(a.zip_code, 0, 5) AS zip_code, a.transaction_pgi, a.amndt_ind, a.image_num, a.file_num, a.tran_id, a.line_num, a.rpt_yr, a.rpt_tp, a.form_tp_cd, a.sched_tp_cd, null AS rec_dt, null AS prev_file_num, a.back_ref_tran_id, a.sub_id
    FROM `federal_fec.oppexp22` a
    LEFT JOIN `federal_fec.cm22` b
    ON a.cmte_id = b.cmte_id
    WHERE a.memo_cd IS NULL
    UNION ALL
    SELECT CONCAT(CAST(a.file_num AS STRING), '-' , a.tra_id), 'independent', a.spe_id, IFNULL(b.cmte_nm, a.spe_nam), CAST(PARSE_DATE('%d-%b-%y', CASE WHEN a.exp_dat = "" THEN null ELSE a.exp_dat END) AS STRING), a.exp_amo, a.pur, a.sup_opp, a.can_id, IFNULL(c.cand_name, a.can_nam), SUBSTR(IFNULL(c.cand_pty_affiliation, a.can_par_aff), 0, 3), IFNULL(c.cand_election_yr, a.fec_election_yr), IFNULL(c.cand_office_st, a.can_off_sta), IFNULL(c.cand_office, a.can_off), IFNULL(c.cand_office_district, a.can_off_dis), null, null, a.pay, null, null, null, CONCAT(a.ele_typ, a.fec_election_yr), a.amn_ind, a.ima_num, a.file_num, a.tra_id, null, null, null, null, null, CAST(PARSE_DATE('%d-%b-%y', CASE WHEN a.rec_dt = "" THEN null ELSE a.rec_dt END) AS STRING), a.prev_file_num, null, null
    FROM `federal_fec.independent_expenditure_2022` a
    LEFT JOIN `federal_fec.cm22` b
    ON a.spe_id = b.cmte_id
    LEFT JOIN `federal_fec.cn22` c
    ON a.can_id = c.cand_id
    """, job_config=job_config)
    expenditures_job.result()
    assert expenditures_job.state == "DONE"
    logger.info(' - '.join(['INFO', 'expenditures table loaded']))

    # return true
    return True
