import logging
from google.cloud import secretmanager
from google.cloud import bigquery

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
client = bigquery.Client()

# imports FEC file from Google Cloud Storage into bigquery
def federal_fec_ingest_import_bigquery(message, context):

    # get the filepath from the Pub/Sub message
    filepath = message['attributes']['filepath']

    # set up bigquery
    dataset_ref = client.dataset('federal_fec')
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    if ".txt" in filepath:
        job_config.field_delimiter = "|"
        job_config.quote_character = ""
    else:
        job_config.skip_leading_rows = 1
        job_config.quote_character = "\""

    # set up load job configuration
    uri = "gs://" + gcp_project_id + "/downloads/federal/fec/" + filepath
    table = filepath.split('/')[0]
    destination_table_ref = dataset_ref.table(table)
    archive_table_ref = dataset_ref.table(table + "_old")
    job_config.write_disposition = 'WRITE_TRUNCATE'
    job_config.schema = []

    # set schema
    if table[:6] == "weball":
        job_config.schema = [
            bigquery.SchemaField("cand_id", "STRING"),
            bigquery.SchemaField("cand_name", "STRING"),
            bigquery.SchemaField("cand_ici", "STRING"),
            bigquery.SchemaField("pty_cd", "STRING"),
            bigquery.SchemaField("cand_pty_affiliation", "STRING"),
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
            bigquery.SchemaField("cand_office_st", "STRING"),
            bigquery.SchemaField("cand_office_district", "STRING"),
            bigquery.SchemaField("spec_election", "STRING"),
            bigquery.SchemaField("prim_election", "STRING"),
            bigquery.SchemaField("run_election", "STRING"),
            bigquery.SchemaField("gen_election", "STRING"),
            bigquery.SchemaField("gen_election_precent", "FLOAT"),
            bigquery.SchemaField("other_pol_cmte_contrib", "FLOAT"),
            bigquery.SchemaField("pol_pty_contrib", "FLOAT"),
            bigquery.SchemaField("cvg_end_dt", "STRING"),
            bigquery.SchemaField("indiv_refunds", "FLOAT"),
            bigquery.SchemaField("cmte_refunds", "FLOAT")
        ]
    elif table[:2] == "cn":
        job_config.schema = [
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
            bigquery.SchemaField("cand_st1", "STRING"),
            bigquery.SchemaField("cand_st2", "STRING"),
            bigquery.SchemaField("cand_city", "STRING"),
            bigquery.SchemaField("cand_st", "STRING"),
            bigquery.SchemaField("cand_zip", "STRING")
        ]
    elif table[:3] == "ccl":
        job_config.schema = [
            bigquery.SchemaField("cand_id", "STRING"),
            bigquery.SchemaField("cand_election_yr", "INTEGER"),
            bigquery.SchemaField("fec_election_yr", "INTEGER"),
            bigquery.SchemaField("cmte_id", "STRING"),
            bigquery.SchemaField("cmte_tp", "STRING"),
            bigquery.SchemaField("cmte_dsgn", "STRING"),
            bigquery.SchemaField("linkage_id", "INTEGER")
        ]
    elif table[:4] == "webl":
        job_config.schema = [
            bigquery.SchemaField("cand_id", "STRING"),
            bigquery.SchemaField("cand_name", "STRING"),
            bigquery.SchemaField("cand_ici", "STRING"),
            bigquery.SchemaField("pty_cd", "STRING"),
            bigquery.SchemaField("cand_pty_affiliation", "STRING"),
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
            bigquery.SchemaField("cand_office_st", "STRING"),
            bigquery.SchemaField("cand_office_district", "STRING"),
            bigquery.SchemaField("spec_election", "STRING"),
            bigquery.SchemaField("prim_election", "STRING"),
            bigquery.SchemaField("run_election", "STRING"),
            bigquery.SchemaField("gen_election", "STRING"),
            bigquery.SchemaField("gen_election_precent", "FLOAT"),
            bigquery.SchemaField("other_pol_cmte_contrib", "FLOAT"),
            bigquery.SchemaField("pol_pty_contrib", "FLOAT"),
            bigquery.SchemaField("cvg_end_dt", "STRING"),
            bigquery.SchemaField("indiv_refunds", "FLOAT"),
            bigquery.SchemaField("cmte_refunds", "FLOAT")
        ]
    elif table[:2] == "cm":
        job_config.schema = [
            bigquery.SchemaField("cmte_id", "STRING"),
            bigquery.SchemaField("cmte_nm", "STRING"),
            bigquery.SchemaField("tres_nm", "STRING"),
            bigquery.SchemaField("cmte_st1", "STRING"),
            bigquery.SchemaField("cmte_st2", "STRING"),
            bigquery.SchemaField("cmte_city", "STRING"),
            bigquery.SchemaField("cmte_st", "STRING"),
            bigquery.SchemaField("cmte_zip", "STRING"),
            bigquery.SchemaField("cmte_dsgn", "STRING"),
            bigquery.SchemaField("cmte_tp", "STRING"),
            bigquery.SchemaField("cmte_pty_affiliation", "STRING"),
            bigquery.SchemaField("cmte_filing_freq", "STRING"),
            bigquery.SchemaField("org_tp", "STRING"),
            bigquery.SchemaField("connected_org_nm", "STRING"),
            bigquery.SchemaField("cand_id", "STRING")
        ]
    elif table[:4] == "webk":
        job_config.schema = [
            bigquery.SchemaField("cmte_id", "STRING"),
            bigquery.SchemaField("cmte_nm", "STRING"),
            bigquery.SchemaField("cmte_tp", "STRING"),
            bigquery.SchemaField("cmte_dsgn", "STRING"),
            bigquery.SchemaField("cmte_filing_freq", "STRING"),
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
        ]
    elif table[:5] == "indiv":
        job_config.schema = [
            bigquery.SchemaField("cmte_id", "STRING"),
            bigquery.SchemaField("amndt_ind", "STRING"),
            bigquery.SchemaField("rpt_tp", "STRING"),
            bigquery.SchemaField("transaction_pgi", "STRING"),
            bigquery.SchemaField("image_num", "STRING"),
            bigquery.SchemaField("transaction_tp", "STRING"),
            bigquery.SchemaField("entity_tp", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("zip_code", "STRING"),
            bigquery.SchemaField("employer", "STRING"),
            bigquery.SchemaField("occupation", "STRING"),
            bigquery.SchemaField("transaction_dt", "STRING"),
            bigquery.SchemaField("transaction_amt", "FLOAT"),
            bigquery.SchemaField("other_id", "STRING"),
            bigquery.SchemaField("tran_id", "STRING"),
            bigquery.SchemaField("file_num", "INTEGER"),
            bigquery.SchemaField("memo_cd", "STRING"),
            bigquery.SchemaField("memo_text", "STRING"),
            bigquery.SchemaField("sub_id", "INTEGER")
        ]
    elif table[:3] == "pas":
        job_config.schema = [
            bigquery.SchemaField("cmte_id", "STRING"),
            bigquery.SchemaField("amndt_ind", "STRING"),
            bigquery.SchemaField("rpt_tp", "STRING"),
            bigquery.SchemaField("transaction_pgi", "STRING"),
            bigquery.SchemaField("image_num", "STRING"),
            bigquery.SchemaField("transaction_tp", "STRING"),
            bigquery.SchemaField("entity_tp", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("zip_code", "STRING"),
            bigquery.SchemaField("employer", "STRING"),
            bigquery.SchemaField("occupation", "STRING"),
            bigquery.SchemaField("transaction_dt", "STRING"),
            bigquery.SchemaField("transaction_amt", "FLOAT"),
            bigquery.SchemaField("other_id", "STRING"),
            bigquery.SchemaField("cand_id", "STRING"),
            bigquery.SchemaField("tran_id", "STRING"),
            bigquery.SchemaField("file_num", "INTEGER"),
            bigquery.SchemaField("memo_cd", "STRING"),
            bigquery.SchemaField("memo_text", "STRING"),
            bigquery.SchemaField("sub_id", "INTEGER")
        ]
    elif table[:3] == "oth":
        job_config.schema = [
            bigquery.SchemaField("cmte_id", "STRING"),
            bigquery.SchemaField("amndt_ind", "STRING"),
            bigquery.SchemaField("rpt_tp", "STRING"),
            bigquery.SchemaField("transaction_pgi", "STRING"),
            bigquery.SchemaField("image_num", "STRING"),
            bigquery.SchemaField("transaction_tp", "STRING"),
            bigquery.SchemaField("entity_tp", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("zip_code", "STRING"),
            bigquery.SchemaField("employer", "STRING"),
            bigquery.SchemaField("occupation", "STRING"),
            bigquery.SchemaField("transaction_dt", "STRING"),
            bigquery.SchemaField("transaction_amt", "FLOAT"),
            bigquery.SchemaField("other_id", "STRING"),
            bigquery.SchemaField("tran_id", "STRING"),
            bigquery.SchemaField("file_num", "INTEGER"),
            bigquery.SchemaField("memo_cd", "STRING"),
            bigquery.SchemaField("memo_text", "STRING"),
            bigquery.SchemaField("sub_id", "INTEGER")
        ]
    elif table[:6] == "oppexp":
        job_config.schema = [
            bigquery.SchemaField("cmte_id", "STRING"),
            bigquery.SchemaField("amndt_ind", "STRING"),
            bigquery.SchemaField("rpt_yr", "INTEGER"),
            bigquery.SchemaField("rpt_tp", "STRING"),
            bigquery.SchemaField("image_num", "STRING"),
            bigquery.SchemaField("line_num", "STRING"),
            bigquery.SchemaField("form_tp_cd", "STRING"),
            bigquery.SchemaField("sched_tp_cd", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("zip_code", "STRING"),
            bigquery.SchemaField("transaction_dt", "STRING"),
            bigquery.SchemaField("transaction_amt", "FLOAT"),
            bigquery.SchemaField("transaction_pgi", "STRING"),
            bigquery.SchemaField("purpose", "STRING"),
            bigquery.SchemaField("category", "STRING"),
            bigquery.SchemaField("category_desc", "STRING"),
            bigquery.SchemaField("memo_cd", "STRING"),
            bigquery.SchemaField("memo_text", "STRING"),
            bigquery.SchemaField("entity_tp", "STRING"),
            bigquery.SchemaField("sub_id", "INTEGER"),
            bigquery.SchemaField("file_num", "INTEGER"),
            bigquery.SchemaField("tran_id", "STRING"),
            bigquery.SchemaField("back_ref_tran_id", "STRING"),
            bigquery.SchemaField("empty", "STRING")
        ]
    elif table[:23] == "independent_expenditure":
        job_config.schema = [
            bigquery.SchemaField("can_id", "STRING"),
            bigquery.SchemaField("can_nam", "STRING"),
            bigquery.SchemaField("spe_id", "STRING"),
            bigquery.SchemaField("spe_nam", "STRING"),
            bigquery.SchemaField("ele_typ", "STRING"),
            bigquery.SchemaField("can_off_sta", "STRING"),
            bigquery.SchemaField("can_off_dis", "STRING"),
            bigquery.SchemaField("can_off", "STRING"),
            bigquery.SchemaField("can_par_aff", "STRING"),
            bigquery.SchemaField("exp_amo", "FLOAT"),
            bigquery.SchemaField("exp_dat", "STRING"),
            bigquery.SchemaField("agg_amo", "FLOAT"),
            bigquery.SchemaField("sup_opp", "STRING"),
            bigquery.SchemaField("pur", "STRING"),
            bigquery.SchemaField("pay", "STRING"),
            bigquery.SchemaField("file_num", "INTEGER"),
            bigquery.SchemaField("amn_ind", "STRING"),
            bigquery.SchemaField("tra_id", "STRING"),
            bigquery.SchemaField("ima_num", "STRING"),
            bigquery.SchemaField("rec_dt", "STRING"),
            bigquery.SchemaField("fec_election_yr", "INTEGER"),
            bigquery.SchemaField("prev_file_num", "INTEGER"),
            bigquery.SchemaField("dissem_dt", "STRING")
        ]
    elif table[:18] == "ElectioneeringComm":
        job_config.schema = [
            bigquery.SchemaField("candidate_id", "STRING"),
            bigquery.SchemaField("candidate_name", "STRING"),
            bigquery.SchemaField("candidate_office", "STRING"),
            bigquery.SchemaField("candidate_state", "STRING"),
            bigquery.SchemaField("candidate_office_district", "STRING"),
            bigquery.SchemaField("committee_id", "STRING"),
            bigquery.SchemaField("committee_name", "STRING"),
            bigquery.SchemaField("sb_image_num", "STRING"),
            bigquery.SchemaField("payee_name", "STRING"),
            bigquery.SchemaField("payee_street", "STRING"),
            bigquery.SchemaField("payee_city", "STRING"),
            bigquery.SchemaField("payee_state", "STRING"),
            bigquery.SchemaField("disbursement_description", "STRING"),
            bigquery.SchemaField("disbursement_date", "STRING"),
            bigquery.SchemaField("communication_date", "STRING"),
            bigquery.SchemaField("public_distribution_date", "STRING"),
            bigquery.SchemaField("reported_disbursement_amount", "FLOAT"),
            bigquery.SchemaField("number_of_candidates", "INTEGER"),
            bigquery.SchemaField("calculated_candidate_share", "FLOAT")
        ]
    elif table[:18] == "CommunicationCosts":
        job_config.schema = [
            bigquery.SchemaField("cmte_id", "STRING"),
            bigquery.SchemaField("cmte_name", "STRING"),
            bigquery.SchemaField("candidate_id", "STRING"),
            bigquery.SchemaField("candidate_name", "STRING"),
            bigquery.SchemaField("candidate_office", "STRING"),
            bigquery.SchemaField("candidate_office_state", "STRING"),
            bigquery.SchemaField("candidate_office_district", "STRING"),
            bigquery.SchemaField("cand_pty_affiliation", "STRING"),
            bigquery.SchemaField("transaction_dt", "STRING"),
            bigquery.SchemaField("transaction_amt", "FLOAT"),
            bigquery.SchemaField("transaction_tp", "STRING"),
            bigquery.SchemaField("communication_tp", "STRING"),
            bigquery.SchemaField("communication_class", "STRING"),
            bigquery.SchemaField("support_oppose_ind", "STRING"),
            bigquery.SchemaField("image_num", "STRING"),
            bigquery.SchemaField("line_num", "INTEGER"),
            bigquery.SchemaField("form_tp_cd", "STRING"),
            bigquery.SchemaField("sched_tp_cd", "STRING"),
            bigquery.SchemaField("tran_id", "STRING"),
            bigquery.SchemaField("sub_id", "INTEGER"),
            bigquery.SchemaField("file_num", "INTEGER"),
            bigquery.SchemaField("rpt_yr", "INTEGER"),
            bigquery.SchemaField("cand_state_description", "STRING"),
            bigquery.SchemaField("cand_pty_affiliation_description", "STRING"),
            bigquery.SchemaField("purpose", "STRING")
        ]

    if job_config.schema is not []:

        # archive old table
        destination_table = client.get_table(destination_table_ref)
        archive_table = None
        if destination_table.num_rows > 0:
            # delete old archive table
            client.delete_table(archive_table_ref, not_found_ok=True)
            # copy the old table to archive it
            copy_job = client.copy_table(destination_table_ref, archive_table_ref)
            logger.info(' - '.join(['START', 'copying old table', table, copy_job.job_id]))
            copy_job.result()
            assert copy_job.state == "DONE"
            archive_table = client.get_table(archive_table_ref)
            logger.info(' - '.join(['INFO', 'old table archived', table, str(archive_table.num_rows)]))

        # load new file
        load_job = client.load_table_from_uri(uri, destination_table_ref, job_config=job_config)
        logger.info(' - '.join(['START', 'load new file', table, load_job.job_id]))
        load_job.result()
        assert load_job.state == "DONE"
        destination_table = client.get_table(destination_table_ref)
        logger.info(' - '.join(['INFO', 'new file loaded', table, str(destination_table.num_rows)]))

        # validate data
        if archive_table is not None:
            if destination_table.num_rows >= archive_table.num_rows:
                logger.info(' - '.join(['COMPLETED', 'file imported', filepath]))
                # delete archive table
                client.delete_table(archive_table_ref)
            else:
                logger.error(' - '.join(['INFO', 'rolling back', filepath]))
                # delete new table
                client.delete_table(destination_table_ref)
                # copy back archived table
                copy_job = client.copy_table(archive_table_ref, destination_table_ref)
                copy_job.result()
                assert copy_job.state == "DONE"
                logger.error(' - '.join(['ERROR', 'validation failed', filepath]))

    else:

        logger.error(' - '.join(['ERROR', 'unexpected file', filepath]))

    # return the filepath
    return filepath
