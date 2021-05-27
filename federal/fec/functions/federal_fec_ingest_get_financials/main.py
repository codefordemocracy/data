import logging
from google.cloud import secretmanager
import requests
import datetime
import pytz
import json
import time
from google.cloud import bigquery

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.INFO)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
federal_fec_api_key = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/federal_fec_api_key/versions/1"}).payload.data.decode()

# connect to fec api
def get(type, datestring, page):
    response = requests.get('https://api.open.fec.gov/v1/reports/' + type + '/?per_page=100&api_key=' + federal_fec_api_key + '&min_receipt_date=' + datestring + '&max_receipt_date=' + datestring + '&page=' + str(page))
    if response.status_code == 200:
        return json.loads(response.text)
    return False

# helper function to return None if key not in row
def clean_row(row, key):
    if key in row:
        return row[key]
    return None

# helper function to get values string from array of rows
def get_values_string_rows(rows):
    values_string = ""
    for row in rows:
        values_string += "("
        values = [
            clean_row(row, "aggregate_amount_personal_contributions_general"),
            clean_row(row, "aggregate_contributions_personal_funds_primary"),
            clean_row(row, "all_loans_received_period"),
            clean_row(row, "all_loans_received_ytd"),
            clean_row(row, "all_other_loans_period"),
            clean_row(row, "all_other_loans_ytd"),
            clean_row(row, "allocated_federal_election_levin_share_period"),
            clean_row(row, "amendment_chain"),
            clean_row(row, "amendment_indicator"),
            clean_row(row, "amendment_indicator_full"),
            clean_row(row, "beginning_image_number"),
            clean_row(row, "calendar_ytd"),
            clean_row(row, "candidate_contribution_period"),
            clean_row(row, "candidate_contribution_ytd"),
            clean_row(row, "cash_on_hand_beginning_calendar_ytd"),
            clean_row(row, "cash_on_hand_beginning_period"),
            clean_row(row, "cash_on_hand_close_ytd"),
            clean_row(row, "cash_on_hand_end_period"),
            clean_row(row, "committee_id"),
            clean_row(row, "committee_name"),
            clean_row(row, "committee_type"),
            clean_row(row, "coordinated_expenditures_by_party_committee_period"),
            clean_row(row, "coordinated_expenditures_by_party_committee_ytd"),
            clean_row(row, "coverage_end_date"),
            clean_row(row, "coverage_start_date"),
            clean_row(row, "csv_url"),
            clean_row(row, "cycle"),
            clean_row(row, "debts_owed_by_committee"),
            clean_row(row, "debts_owed_to_committee"),
            clean_row(row, "document_description"),
            clean_row(row, "end_image_number"),
            clean_row(row, "exempt_legal_accounting_disbursement_period"),
            clean_row(row, "exempt_legal_accounting_disbursement_ytd"),
            clean_row(row, "expenditure_subject_to_limits"),
            clean_row(row, "fec_file_id"),
            clean_row(row, "fec_url"),
            clean_row(row, "fed_candidate_committee_contribution_refunds_ytd"),
            clean_row(row, "fed_candidate_committee_contributions_period"),
            clean_row(row, "fed_candidate_committee_contributions_ytd"),
            clean_row(row, "fed_candidate_contribution_refunds_period"),
            clean_row(row, "federal_funds_period"),
            clean_row(row, "federal_funds_ytd"),
            clean_row(row, "file_number"),
            clean_row(row, "fundraising_disbursements_period"),
            clean_row(row, "fundraising_disbursements_ytd"),
            clean_row(row, "gross_receipt_authorized_committee_general"),
            clean_row(row, "gross_receipt_authorized_committee_primary"),
            clean_row(row, "gross_receipt_minus_personal_contribution_general"),
            clean_row(row, "gross_receipt_minus_personal_contributions_primary"),
            clean_row(row, "html_url"),
            clean_row(row, "independent_contributions_period"),
            clean_row(row, "independent_expenditures_period"),
            clean_row(row, "independent_expenditures_ytd"),
            clean_row(row, "individual_itemized_contributions_period"),
            clean_row(row, "individual_itemized_contributions_ytd"),
            clean_row(row, "individual_unitemized_contributions_period"),
            clean_row(row, "individual_unitemized_contributions_ytd"),
            clean_row(row, "is_amended"),
            clean_row(row, "items_on_hand_liquidated"),
            clean_row(row, "loan_repayments_candidate_loans_period"),
            clean_row(row, "loan_repayments_candidate_loans_ytd"),
            clean_row(row, "loan_repayments_made_period"),
            clean_row(row, "loan_repayments_made_ytd"),
            clean_row(row, "loan_repayments_other_loans_period"),
            clean_row(row, "loan_repayments_other_loans_ytd"),
            clean_row(row, "loan_repayments_received_period"),
            clean_row(row, "loan_repayments_received_ytd"),
            clean_row(row, "loans_made_by_candidate_period"),
            clean_row(row, "loans_made_by_candidate_ytd"),
            clean_row(row, "loans_made_period"),
            clean_row(row, "loans_made_ytd"),
            clean_row(row, "loans_received_from_candidate_period"),
            clean_row(row, "loans_received_from_candidate_ytd"),
            clean_row(row, "means_filed"),
            clean_row(row, "most_recent"),
            clean_row(row, "most_recent_file_number"),
            clean_row(row, "net_contributions_cycle_to_date"),
            clean_row(row, "net_contributions_period"),
            clean_row(row, "net_contributions_ytd"),
            clean_row(row, "net_operating_expenditures_cycle_to_date"),
            clean_row(row, "net_operating_expenditures_period"),
            clean_row(row, "net_operating_expenditures_ytd"),
            clean_row(row, "non_allocated_fed_election_activity_period"),
            clean_row(row, "non_allocated_fed_election_activity_ytd"),
            clean_row(row, "nonfed_share_allocated_disbursements_period"),
            clean_row(row, "offsets_to_fundraising_expenditures_period"),
            clean_row(row, "offsets_to_fundraising_expenditures_ytd"),
            clean_row(row, "offsets_to_legal_accounting_period"),
            clean_row(row, "offsets_to_legal_accounting_ytd"),
            clean_row(row, "offsets_to_operating_expenditures_period"),
            clean_row(row, "offsets_to_operating_expenditures_ytd"),
            clean_row(row, "operating_expenditures_period"),
            clean_row(row, "operating_expenditures_ytd"),
            clean_row(row, "other_disbursements_period"),
            clean_row(row, "other_disbursements_ytd"),
            clean_row(row, "other_fed_operating_expenditures_period"),
            clean_row(row, "other_fed_operating_expenditures_ytd"),
            clean_row(row, "other_fed_receipts_period"),
            clean_row(row, "other_fed_receipts_ytd"),
            clean_row(row, "other_loans_received_period"),
            clean_row(row, "other_loans_received_ytd"),
            clean_row(row, "other_political_committee_contributions_period"),
            clean_row(row, "other_political_committee_contributions_ytd"),
            clean_row(row, "other_receipts_period"),
            clean_row(row, "other_receipts_ytd"),
            clean_row(row, "pdf_url"),
            clean_row(row, "political_party_committee_contributions_period"),
            clean_row(row, "political_party_committee_contributions_ytd"),
            clean_row(row, "previous_file_number"),
            clean_row(row, "receipt_date"),
            clean_row(row, "refunded_individual_contributions_period"),
            clean_row(row, "refunded_individual_contributions_ytd"),
            clean_row(row, "refunded_other_political_committee_contributions_period"),
            clean_row(row, "refunded_other_political_committee_contributions_ytd"),
            clean_row(row, "refunded_political_party_committee_contributions_period"),
            clean_row(row, "refunded_political_party_committee_contributions_ytd"),
            clean_row(row, "refunds_total_contributions_col_total_ytd"),
            clean_row(row, "repayments_loans_made_by_candidate_period"),
            clean_row(row, "repayments_loans_made_candidate_ytd"),
            clean_row(row, "repayments_other_loans_period"),
            clean_row(row, "repayments_other_loans_ytd"),
            clean_row(row, "report_form"),
            clean_row(row, "report_type"),
            clean_row(row, "report_type_full"),
            clean_row(row, "report_year"),
            clean_row(row, "shared_fed_activity_nonfed_ytd"),
            clean_row(row, "shared_fed_activity_period"),
            clean_row(row, "shared_fed_activity_ytd"),
            clean_row(row, "shared_fed_operating_expenditures_period"),
            clean_row(row, "shared_fed_operating_expenditures_ytd"),
            clean_row(row, "shared_nonfed_operating_expenditures_period"),
            clean_row(row, "shared_nonfed_operating_expenditures_ytd"),
            clean_row(row, "subtotal_period"),
            clean_row(row, "subtotal_summary_page_period"),
            clean_row(row, "subtotal_summary_period"),
            clean_row(row, "subtotal_summary_ytd"),
            clean_row(row, "total_contribution_refunds_col_total_period"),
            clean_row(row, "total_contribution_refunds_period"),
            clean_row(row, "total_contribution_refunds_ytd"),
            clean_row(row, "total_contributions_column_total_period"),
            clean_row(row, "total_contributions_period"),
            clean_row(row, "total_contributions_ytd"),
            clean_row(row, "total_disbursements_period"),
            clean_row(row, "total_disbursements_ytd"),
            clean_row(row, "total_fed_disbursements_period"),
            clean_row(row, "total_fed_disbursements_ytd"),
            clean_row(row, "total_fed_election_activity_period"),
            clean_row(row, "total_fed_election_activity_ytd"),
            clean_row(row, "total_fed_operating_expenditures_period"),
            clean_row(row, "total_fed_operating_expenditures_ytd"),
            clean_row(row, "total_fed_receipts_period"),
            clean_row(row, "total_fed_receipts_ytd"),
            clean_row(row, "total_individual_contributions_period"),
            clean_row(row, "total_individual_contributions_ytd"),
            clean_row(row, "total_loan_repayments_made_period"),
            clean_row(row, "total_loan_repayments_made_ytd"),
            clean_row(row, "total_loans_received_period"),
            clean_row(row, "total_loans_received_ytd"),
            clean_row(row, "total_nonfed_transfers_period"),
            clean_row(row, "total_nonfed_transfers_ytd"),
            clean_row(row, "total_offsets_to_operating_expenditures_period"),
            clean_row(row, "total_offsets_to_operating_expenditures_ytd"),
            clean_row(row, "total_operating_expenditures_period"),
            clean_row(row, "total_operating_expenditures_ytd"),
            clean_row(row, "total_period"),
            clean_row(row, "total_receipts_period"),
            clean_row(row, "total_receipts_ytd"),
            clean_row(row, "total_ytd"),
            clean_row(row, "transfers_from_affiliated_committee_period"),
            clean_row(row, "transfers_from_affiliated_committee_ytd"),
            clean_row(row, "transfers_from_affiliated_party_period"),
            clean_row(row, "transfers_from_affiliated_party_ytd"),
            clean_row(row, "transfers_from_nonfed_account_period"),
            clean_row(row, "transfers_from_nonfed_account_ytd"),
            clean_row(row, "transfers_from_nonfed_levin_period"),
            clean_row(row, "transfers_from_nonfed_levin_ytd"),
            clean_row(row, "transfers_from_other_authorized_committee_period"),
            clean_row(row, "transfers_from_other_authorized_committee_ytd"),
            clean_row(row, "transfers_to_affiliated_committee_period"),
            clean_row(row, "transfers_to_affilitated_committees_ytd"),
            clean_row(row, "transfers_to_other_authorized_committee_period"),
            clean_row(row, "transfers_to_other_authorized_committee_ytd")
        ]
        for value in values:
            if value is None:
                values_string += "null"
            elif isinstance(value, list):
                array_string = json.dumps(value).replace("\\", "\\\\'").replace("'", "\\'")
                values_string += "'" + array_string + "'"
            else:
                try:
                    value = value.replace("\\", "\\\\'").replace("'", "\\'")
                    values_string += "'" + value + "'"
                except:
                    values_string += str(value)
            values_string += ", "
        values_string = values_string[:-2]
        values_string += "), "
    values_string = values_string[:-2]
    return values_string

# set up bigquery
client = bigquery.Client()

# helper function to generate a new job config
def gen_job_config():
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    return job_config

# helper function to delete data from date
def delete_receipt_date(receipt_date_str):
    # set table
    year = int(receipt_date_str[2:4])
    if year % 2 > 0:
        year = year + 1
    table = "financials" + str(year).zfill(2)
    # delete from table
    query_job = client.query("""
    DELETE
    FROM `federal_fec.%s`
    WHERE receipt_date = '%s'
    """ % (table, receipt_date_str), job_config=gen_job_config())
    query_job.result()
    query_job.state == "DONE"

# helper function to insert rows into bigquery
def commit_rows(rows, datestring):
    # get values
    values_string = get_values_string_rows(rows)
    # set table
    year = int(datestring[2:4])
    if year % 2 > 0:
        year = year + 1
    table = "financials" + str(year).zfill(2)
    # insert data
    query_job = client.query("""
    INSERT INTO `federal_fec.%s` (aggregate_amount_personal_contributions_general,aggregate_contributions_personal_funds_primary,all_loans_received_period,all_loans_received_ytd,all_other_loans_period,all_other_loans_ytd,allocated_federal_election_levin_share_period,amendment_chain,amendment_indicator,amendment_indicator_full,beginning_image_number,calendar_ytd,candidate_contribution_period,candidate_contribution_ytd,cash_on_hand_beginning_calendar_ytd,cash_on_hand_beginning_period,cash_on_hand_close_ytd,cash_on_hand_end_period,committee_id,committee_name,committee_type,coordinated_expenditures_by_party_committee_period,coordinated_expenditures_by_party_committee_ytd,coverage_end_date,coverage_start_date,csv_url,cycle,debts_owed_by_committee,debts_owed_to_committee,document_description,end_image_number,exempt_legal_accounting_disbursement_period,exempt_legal_accounting_disbursement_ytd,expenditure_subject_to_limits,fec_file_id,fec_url,fed_candidate_committee_contribution_refunds_ytd,fed_candidate_committee_contributions_period,fed_candidate_committee_contributions_ytd,fed_candidate_contribution_refunds_period,federal_funds_period,federal_funds_ytd,file_number,fundraising_disbursements_period,fundraising_disbursements_ytd,gross_receipt_authorized_committee_general,gross_receipt_authorized_committee_primary,gross_receipt_minus_personal_contribution_general,gross_receipt_minus_personal_contributions_primary,html_url,independent_contributions_period,independent_expenditures_period,independent_expenditures_ytd,individual_itemized_contributions_period,individual_itemized_contributions_ytd,individual_unitemized_contributions_period,individual_unitemized_contributions_ytd,is_amended,items_on_hand_liquidated,loan_repayments_candidate_loans_period,loan_repayments_candidate_loans_ytd,loan_repayments_made_period,loan_repayments_made_ytd,loan_repayments_other_loans_period,loan_repayments_other_loans_ytd,loan_repayments_received_period,loan_repayments_received_ytd,loans_made_by_candidate_period,loans_made_by_candidate_ytd,loans_made_period,loans_made_ytd,loans_received_from_candidate_period,loans_received_from_candidate_ytd,means_filed,most_recent,most_recent_file_number,net_contributions_cycle_to_date,net_contributions_period,net_contributions_ytd,net_operating_expenditures_cycle_to_date,net_operating_expenditures_period,net_operating_expenditures_ytd,non_allocated_fed_election_activity_period,non_allocated_fed_election_activity_ytd,nonfed_share_allocated_disbursements_period,offsets_to_fundraising_expenditures_period,offsets_to_fundraising_expenditures_ytd,offsets_to_legal_accounting_period,offsets_to_legal_accounting_ytd,offsets_to_operating_expenditures_period,offsets_to_operating_expenditures_ytd,operating_expenditures_period,operating_expenditures_ytd,other_disbursements_period,other_disbursements_ytd,other_fed_operating_expenditures_period,other_fed_operating_expenditures_ytd,other_fed_receipts_period,other_fed_receipts_ytd,other_loans_received_period,other_loans_received_ytd,other_political_committee_contributions_period,other_political_committee_contributions_ytd,other_receipts_period,other_receipts_ytd,pdf_url,political_party_committee_contributions_period,political_party_committee_contributions_ytd,previous_file_number,receipt_date,refunded_individual_contributions_period,refunded_individual_contributions_ytd,refunded_other_political_committee_contributions_period,refunded_other_political_committee_contributions_ytd,refunded_political_party_committee_contributions_period,refunded_political_party_committee_contributions_ytd,refunds_total_contributions_col_total_ytd,repayments_loans_made_by_candidate_period,repayments_loans_made_candidate_ytd,repayments_other_loans_period,repayments_other_loans_ytd,report_form,report_type,report_type_full,report_year,shared_fed_activity_nonfed_ytd,shared_fed_activity_period,shared_fed_activity_ytd,shared_fed_operating_expenditures_period,shared_fed_operating_expenditures_ytd,shared_nonfed_operating_expenditures_period,shared_nonfed_operating_expenditures_ytd,subtotal_period,subtotal_summary_page_period,subtotal_summary_period,subtotal_summary_ytd,total_contribution_refunds_col_total_period,total_contribution_refunds_period,total_contribution_refunds_ytd,total_contributions_column_total_period,total_contributions_period,total_contributions_ytd,total_disbursements_period,total_disbursements_ytd,total_fed_disbursements_period,total_fed_disbursements_ytd,total_fed_election_activity_period,total_fed_election_activity_ytd,total_fed_operating_expenditures_period,total_fed_operating_expenditures_ytd,total_fed_receipts_period,total_fed_receipts_ytd,total_individual_contributions_period,total_individual_contributions_ytd,total_loan_repayments_made_period,total_loan_repayments_made_ytd,total_loans_received_period,total_loans_received_ytd,total_nonfed_transfers_period,total_nonfed_transfers_ytd,total_offsets_to_operating_expenditures_period,total_offsets_to_operating_expenditures_ytd,total_operating_expenditures_period,total_operating_expenditures_ytd,total_period,total_receipts_period,total_receipts_ytd,total_ytd,transfers_from_affiliated_committee_period,transfers_from_affiliated_committee_ytd,transfers_from_affiliated_party_period,transfers_from_affiliated_party_ytd,transfers_from_nonfed_account_period,transfers_from_nonfed_account_ytd,transfers_from_nonfed_levin_period,transfers_from_nonfed_levin_ytd,transfers_from_other_authorized_committee_period,transfers_from_other_authorized_committee_ytd,transfers_to_affiliated_committee_period,transfers_to_affilitated_committees_ytd,transfers_to_other_authorized_committee_period,transfers_to_other_authorized_committee_ytd)
    VALUES %s
    """ % (table, values_string), job_config=gen_job_config())
    query_job.result()
    assert query_job.state == "DONE"
    # log results
    logger.info(' - '.join(['INFO', 'rows inserted', datestring, str(query_job.num_dml_affected_rows)]))

# gets financials from FEC API and loads them into BigQuery
def federal_fec_ingest_get_financials(message, context):

    today = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
    yesterweek = today-datetime.timedelta(days=7)

    if 'attributes' in message:
        if message['attributes'] is not None:
            if "date" in message["attributes"]:
                datestring = message["attributes"]["date"]

    datestring = today.strftime("%Y-%m-%d")
    while datestring != yesterweek.strftime("%Y-%m-%d"):

        # delete existing data for date
        delete_receipt_date(datestring+"T00:00:00")

        # get new data for date
        rows = []
        for type in ["presidential", "pac-party", "house-senate", "ie-only"]:
            page = 1
            num_results = -1
            while num_results != 0:
                response = False
                # get data from fec api
                while response is False:
                    response = get(type, datestring, page)
                    time.sleep(3)
                num_results = len(response["results"])
                logger.info(' - '.join(['INFO', 'api response received', type, datestring, str(page), str(num_results)]))
                # add results to rows
                rows.extend(response["results"])
                if len(rows) >= 150:
                    commit_rows(rows, datestring)
                    rows = []
                # iterate through pages
                page += 1
        # send results to bigquery
        if len(rows) > 0:
            commit_rows(rows, datestring)
        # get next date
        datestring = (datetime.datetime.strptime(datestring, '%Y-%m-%d')-datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    return True
