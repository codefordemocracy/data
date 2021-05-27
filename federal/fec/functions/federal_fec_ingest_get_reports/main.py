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
def get(datestring, page):
    response = requests.get('https://api.open.fec.gov/v1/filings/?per_page=100&api_key=' + federal_fec_api_key + '&min_receipt_date=' + datestring + '&max_receipt_date=' + datestring + '&page=' + str(page))
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
        values = [clean_row(row, "amendment_chain"), clean_row(row, "amendment_indicator"), clean_row(row, "amendment_version"), clean_row(row, "beginning_image_number"), clean_row(row, "candidate_id"), clean_row(row, "candidate_name"), clean_row(row, "cash_on_hand_beginning_period"), clean_row(row, "cash_on_hand_end_period"), clean_row(row, "committee_id"), clean_row(row, "committee_name"), clean_row(row, "committee_type"), clean_row(row, "coverage_end_date"), clean_row(row, "coverage_start_date"), clean_row(row, "csv_url"), clean_row(row, "cycle"), clean_row(row, "debts_owed_by_committee"), clean_row(row, "debts_owed_to_committee"), clean_row(row, "document_description"), clean_row(row, "document_type"), clean_row(row, "document_type_full"), clean_row(row, "election_year"), clean_row(row, "ending_image_number"), clean_row(row, "fec_file_id"), clean_row(row, "fec_url"), clean_row(row, "file_number"), clean_row(row, "form_category"), clean_row(row, "form_type"), clean_row(row, "house_personal_funds"), clean_row(row, "html_url"), clean_row(row, "is_amended"), clean_row(row, "means_filed"), clean_row(row, "most_recent"), clean_row(row, "most_recent_file_number"), clean_row(row, "net_donations"), clean_row(row, "office"), clean_row(row, "opposition_personal_funds"), clean_row(row, "pages"), clean_row(row, "party"), clean_row(row, "pdf_url"), clean_row(row, "previous_file_number"), clean_row(row, "primary_general_indicator"), clean_row(row, "receipt_date"), clean_row(row, "report_type"), clean_row(row, "report_type_full"), clean_row(row, "report_year"), clean_row(row, "request_type"), clean_row(row, "senate_personal_funds"), clean_row(row, "state"), clean_row(row, "sub_id"), clean_row(row, "total_communication_cost"), clean_row(row, "total_disbursements"), clean_row(row, "total_independent_expenditures"), clean_row(row, "total_individual_contributions"), clean_row(row, "total_receipts"), clean_row(row, "treasurer_name"), clean_row(row, "update_date")]
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
    table = "reports" + str(year).zfill(2)
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
    table = "reports" + str(year).zfill(2)
    # insert data
    query_job = client.query("""
    INSERT INTO `federal_fec.%s` (amendment_chain, amendment_indicator, amendment_version, beginning_image_number, candidate_id, candidate_name, cash_on_hand_beginning_period, cash_on_hand_end_period, committee_id, committee_name, committee_type, coverage_end_date, coverage_start_date, csv_url, cycle, debts_owed_by_committee, debts_owed_to_committee, document_description, document_type, document_type_full, election_year, ending_image_number, fec_file_id, fec_url, file_number, form_category, form_type, house_personal_funds, html_url, is_amended, means_filed, most_recent, most_recent_file_number, net_donations, office, opposition_personal_funds, pages, party, pdf_url, previous_file_number, primary_general_indicator, receipt_date, report_type, report_type_full, report_year, request_type, senate_personal_funds, state, sub_id, total_communication_cost, total_disbursements, total_independent_expenditures, total_individual_contributions, total_receipts, treasurer_name, update_date)
    VALUES %s
    """ % (table, values_string), job_config=gen_job_config())
    query_job.result()
    assert query_job.state == "DONE"
    # log results
    logger.info(' - '.join(['INFO', 'rows inserted', datestring, str(query_job.num_dml_affected_rows)]))

# gets reports from FEC API and loads them into BigQuery
def federal_fec_ingest_get_reports(message, context):

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
        page = 1
        rows = []
        num_results = -1
        while num_results != 0:
            response = False
            # get data from fec api
            while response is False:
                response = get(datestring, page)
                time.sleep(3)
            num_results = len(response["results"])
            logger.info(' - '.join(['INFO', 'api response received', datestring, str(page), str(num_results)]))
            # add results to rows
            rows.extend(response["results"])
            if len(rows) >= 300:
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
