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

    # create master candidates table
    table_ref = dataset_ref.table("candidates22")
    logger.info(' - '.join(['INFO', 'deleting candidates table']))
    client.delete_table(table_ref, not_found_ok=True)
    logger.info(' - '.join(['INFO', 'creating candidates table']))
    client.create_table(bigquery.Table(table_ref, schema=[
        bigquery.SchemaField("cand_id", "STRING"),
        bigquery.SchemaField("cand_name", "STRING"),
        bigquery.SchemaField("cand_pty_affiliation", "STRING"),
        bigquery.SchemaField("cand_election_yr", "INTEGER"),
        bigquery.SchemaField("cand_office_st", "STRING"),
        bigquery.SchemaField("cand_office", "STRING"),
        bigquery.SchemaField("cand_office_district", "STRING"),
        bigquery.SchemaField("cand_ici", "STRING"),
        bigquery.SchemaField("cand_status", "STRING"),
        bigquery.SchemaField("cand_pcc", "STRING"),
        bigquery.SchemaField("cand_zip", "STRING"),
        bigquery.SchemaField("ttl_receipts", "FLOAT"),
        bigquery.SchemaField("trans_from_auth", "FLOAT"),
        bigquery.SchemaField("ttl_disb", "FLOAT"),
        bigquery.SchemaField("trans_to_auth", "FLOAT"),
        bigquery.SchemaField("coh_bop", "FLOAT"),
        bigquery.SchemaField("coh_cop", "FLOAT"),
        bigquery.SchemaField("cand_contrib", "FLOAT"),
        bigquery.SchemaField("cand_loans", "FLOAT"),
        bigquery.SchemaField("other_loans", "FLOAT"),
        bigquery.SchemaField("cand_loan_repay", "FLOAT"),
        bigquery.SchemaField("other_loan_repay", "FLOAT"),
        bigquery.SchemaField("debts_owed_by", "FLOAT"),
        bigquery.SchemaField("ttl_indiv_contrib", "FLOAT"),
        bigquery.SchemaField("other_pol_cmte_contrib", "FLOAT"),
        bigquery.SchemaField("pol_pty_contrib", "FLOAT"),
        bigquery.SchemaField("cvg_end_dt", "STRING"),
        bigquery.SchemaField("indiv_refunds", "FLOAT"),
        bigquery.SchemaField("cmte_refunds", "FLOAT")
    ]))
    logger.info(' - '.join(['START', 'loading candidates table']))
    job_config.destination = None
    candidates_job = client.query("""
    INSERT INTO `federal_fec.candidates22` (cand_id, cand_name, cand_pty_affiliation, cand_election_yr, cand_office_st, cand_office, cand_office_district, cand_ici, cand_status, cand_pcc, cand_zip, ttl_receipts, trans_from_auth, ttl_disb, trans_to_auth, coh_bop, coh_cop, cand_contrib, cand_loans, other_loans, cand_loan_repay, other_loan_repay, debts_owed_by, ttl_indiv_contrib, other_pol_cmte_contrib, pol_pty_contrib, cvg_end_dt, indiv_refunds, cmte_refunds)
    SELECT DISTINCT a.cand_id, a.cand_name, a.cand_pty_affiliation, a.cand_election_yr, a.cand_office_st, a.cand_office, a.cand_office_district, a.cand_ici, a.cand_status, a.cand_pcc, SUBSTR(a.cand_zip, 0, 5) AS cand_zip, b.ttl_receipts, b.trans_from_auth, b.ttl_disb, b.trans_to_auth, b.coh_bop, b.coh_cop, b.cand_contrib, b.cand_loans, b.other_loans, b.cand_loan_repay, b.other_loan_repay, b.debts_owed_by, b.ttl_indiv_contrib, b.other_pol_cmte_contrib, b.pol_pty_contrib, CONCAT(SUBSTR(b.cvg_end_dt,7,4),'-',SUBSTR(b.cvg_end_dt,1,2),'-',SUBSTR(b.cvg_end_dt,4,2)) AS cvg_end_dt, b.indiv_refunds, b.cmte_refunds
    FROM `federal_fec.cn22` a
    LEFT JOIN `federal_fec.weball22` b
    ON a.cand_id = b.cand_id
    """, job_config=job_config)
    candidates_job.result()
    assert candidates_job.state == "DONE"
    logger.info(' - '.join(['INFO', 'candidates table loaded']))

    # create master committees table
    table_ref = dataset_ref.table("committees22")
    logger.info(' - '.join(['INFO', 'deleting committees table']))
    client.delete_table(table_ref, not_found_ok=True)
    logger.info(' - '.join(['INFO', 'creating committees table']))
    client.create_table(bigquery.Table(table_ref, schema=[
        bigquery.SchemaField("cmte_id", "STRING"),
        bigquery.SchemaField("cmte_nm", "STRING"),
        bigquery.SchemaField("cmte_zip", "STRING"),
        bigquery.SchemaField("cmte_dsgn", "STRING"),
        bigquery.SchemaField("cmte_tp", "STRING"),
        bigquery.SchemaField("cmte_pty_affiliation", "STRING"),
        bigquery.SchemaField("cmte_filing_freq", "STRING"),
        bigquery.SchemaField("org_tp", "STRING"),
        bigquery.SchemaField("connected_org_nm", "STRING"),
        bigquery.SchemaField("cand_id", "STRING"),
        bigquery.SchemaField("ttl_receipts", "FLOAT"),
        bigquery.SchemaField("trans_from_aff", "FLOAT"),
        bigquery.SchemaField("indv_contrib", "FLOAT"),
        bigquery.SchemaField("other_pol_cmte_contrib", "FLOAT"),
        bigquery.SchemaField("cand_contrib", "FLOAT"),
        bigquery.SchemaField("cand_loans", "FLOAT"),
        bigquery.SchemaField("ttl_loans_received", "FLOAT"),
        bigquery.SchemaField("ttl_disb", "FLOAT"),
        bigquery.SchemaField("tranf_to_aff", "FLOAT"),
        bigquery.SchemaField("indv_refunds", "FLOAT"),
        bigquery.SchemaField("other_pol_cmte_refunds", "FLOAT"),
        bigquery.SchemaField("cand_loan_repay", "FLOAT"),
        bigquery.SchemaField("loan_repay", "FLOAT"),
        bigquery.SchemaField("coh_bop", "FLOAT"),
        bigquery.SchemaField("coh_cop", "FLOAT"),
        bigquery.SchemaField("debts_owed_by", "FLOAT"),
        bigquery.SchemaField("nonfed_trans_received", "FLOAT"),
        bigquery.SchemaField("contrib_to_other_cmte", "FLOAT"),
        bigquery.SchemaField("ind_exp", "FLOAT"),
        bigquery.SchemaField("pty_coord_exp", "FLOAT"),
        bigquery.SchemaField("nonfed_share_exp", "FLOAT"),
        bigquery.SchemaField("cvg_end_dt", "STRING")
    ]))
    logger.info(' - '.join(['START', 'loading committees table']))
    job_config.destination = None
    committees_job = client.query("""
    INSERT INTO `federal_fec.committees22` (cmte_id, cmte_nm, cmte_zip, cmte_dsgn, cmte_tp, cmte_pty_affiliation, cmte_filing_freq, org_tp, connected_org_nm, cand_id, ttl_receipts, trans_from_aff, indv_contrib, other_pol_cmte_contrib, cand_contrib, cand_loans, ttl_loans_received, ttl_disb, tranf_to_aff, indv_refunds, other_pol_cmte_refunds, cand_loan_repay, loan_repay, coh_bop, coh_cop, debts_owed_by, nonfed_trans_received, contrib_to_other_cmte, ind_exp, pty_coord_exp, nonfed_share_exp, cvg_end_dt)
    SELECT DISTINCT a.cmte_id, a.cmte_nm, SUBSTR(a.cmte_zip, 0, 5) AS cmte_zip, a.cmte_dsgn, a.cmte_tp, a.cmte_pty_affiliation, a.cmte_filing_freq, a.org_tp, a.connected_org_nm, a.cand_id, b.ttl_receipts, b.trans_from_aff, b.indv_contrib, b.other_pol_cmte_contrib, b.cand_contrib, b.cand_loans, b.ttl_loans_received, b.ttl_disb, b.tranf_to_aff, b.indv_refunds, b.other_pol_cmte_refunds, b.cand_loan_repay, b.loan_repay, b.coh_bop, b.coh_cop, b.debts_owed_by, b.nonfed_trans_received, b.contrib_to_other_cmte, b.ind_exp, b.pty_coord_exp, b.nonfed_share_exp, CONCAT(SUBSTR(b.cvg_end_dt,7,4),'-',SUBSTR(b.cvg_end_dt,1,2),'-',SUBSTR(b.cvg_end_dt,4,2)) AS cvg_end_dt
    FROM `federal_fec.cm22` a
    LEFT JOIN `federal_fec.webk22` b
    ON a.cmte_id = b.cmte_id
    """, job_config=job_config)
    committees_job.result()
    assert committees_job.state == "DONE"
    logger.info(' - '.join(['INFO', 'committees table loaded']))

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
