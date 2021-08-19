import logging
from google.cloud import secretmanager
from google.cloud import firestore
from elasticsearch import Elasticsearch, helpers
import requests
import datetime
import pytz
import json
import math
import time

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
elastic_host = secrets.access_secret_version(request={'name': 'projects/952416783871/secrets/elastic_host/versions/1'}).payload.data.decode()
elastic_username_data = secrets.access_secret_version(request={'name': 'projects/952416783871/secrets/elastic_username_data/versions/1'}).payload.data.decode()
elastic_password_data = secrets.access_secret_version(request={'name': 'projects/952416783871/secrets/elastic_password_data/versions/1'}).payload.data.decode()
federal_fec_api_key = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/federal_fec_api_key/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme='https', port=443)
db = firestore.Client()

# other settings
ref = db.collection('federal').document('fec').collection('schedule_a')
min_cycle = 2016

# connect to fec api
def get(two_year_transaction_period, load_date, last_contribution_receipt_amount, last_index):
    url = 'https://api.open.fec.gov/v1/schedules/schedule_a/?per_page=100&api_key=' + federal_fec_api_key + '&sort=-contribution_receipt_amount&two_year_transaction_period=' + str(two_year_transaction_period)
    if load_date is not None:
        url += '&min_load_date=' + load_date.strftime('%Y-%m-%dT00:00:00.000Z')
        url += '&max_load_date=' + (load_date+datetime.timedelta(days=1)).strftime('%Y-%m-%dT00:00:00.000Z')
    if last_contribution_receipt_amount is not None:
        url += '&last_contribution_receipt_amount=' + str(last_contribution_receipt_amount)
    if last_index is not None:
        url += '&last_index=' + last_index
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text)
    return False

# helper loop
def loop(two_year_transaction_period, load_date, last_contribution_receipt_amount, last_index):
    actions = []
    response = get(two_year_transaction_period, load_date, last_contribution_receipt_amount, last_index)
    for obj in response["results"]:
        processed_name = None
        if obj["is_individual"] is True:
            processed_name = ""
            if obj["contributor_first_name"] is not None:
                processed_name += obj["contributor_first_name"] + " "
            if obj["contributor_middle_name"] is not None:
                processed_name += obj["contributor_middle_name"] + " "
            if obj["contributor_last_name"] is not None:
                processed_name += obj["contributor_last_name"]
            processed_name = processed_name.strip()
        record = {
            "obj": obj,
            "context": {
                "last_augmented": datetime.datetime.now(datetime.timezone.utc),
                "last_indexed": datetime.datetime.now(datetime.timezone.utc)
            }
        }
        if obj["contribution_receipt_date"] is not None or processed_name is not None:
            record["processed"] = dict()
            if obj["contribution_receipt_date"] is not None:
                record["processed"]["date"] = obj["contribution_receipt_date"]
            if processed_name is not None:
                record["processed"]["contributor"] = {
                    "individual": {
                        "name": processed_name
                    }
                }
        actions.append({
            "_op_type": "update",
            "_index": "federal_fec_contributions",
            "_id": obj['sub_id'],
            "doc": record,
            "doc_as_upsert": True
        })
    helpers.bulk(es, actions)
    return response["pagination"]

# helper function to get iterables
def get_iterables(iterables=None, reset=False, advance=False):

    current_cycle = math.ceil(datetime.datetime.now().year/2.)*2
    default_pagination = {
        "count": None,
        "pages": 0,
        "last_indexes": {
            "last_contribution_receipt_amount": None,
            "last_index": None
        }
    }

    # set default values
    if iterables is None:
        iterables = {
            "two_year_transaction_period": current_cycle,
            "load_date": datetime.datetime.now(pytz.timezone('EST')),
            "pagination": default_pagination,
            "complete": False,
            "last_updated": datetime.datetime.now()
        }
        # see if there is already a value in Firestore
        doc = ref.document(iterables["load_date"].strftime('%Y-%m-%d')).get().to_dict()
        if doc is not None:
            iterables = doc
            if iterables["complete"] is True:
                advance = True

    # reset pagination if reset is True
    if reset is True:
        iterables["pagination"] = default_pagination

    # continue looping backwards if advance is true
    if advance is True:

        iterables["pagination"] = default_pagination

        # if there are more cycles to grab, get those
        if iterables["two_year_transaction_period"] - 2 >= min_cycle:
            iterables["two_year_transaction_period"] = iterables["two_year_transaction_period"] - 2
            logger.info(' - '.join(['INFO', 'looping to new cycle', iterables["load_date"].strftime('%Y-%m-%d'), str(iterables["two_year_transaction_period"])]))

        # otherwise, mark the load date as complete
        else:

            # update firestore
            iterables["complete"] = True
            ref.document(iterables["load_date"].strftime('%Y-%m-%d')).set(iterables, merge=True)
            logger.info(' - '.join(['INFO', 'load_date completed', iterables["load_date"].strftime('%Y-%m-%d'), str(iterables["two_year_transaction_period"])]))

            # see if there are incomplete load dates in firestore
            incomplete_iterables = False
            docs = ref.where('complete', '==', False).order_by('load_date', direction=firestore.Query.DESCENDING).limit(1).get()
            for doc in docs:
                incomplete_iterables = doc.to_dict()

            # pull new iterables from firestore if they exist, otherwise, check previous date
            if incomplete_iterables is not False:
                iterables = incomplete_iterables
            else:

                # grab oldest completed iterable
                docs = ref.where('complete', '==', True).order_by('load_date').limit(1).get()
                for doc in docs:
                    iterables = doc.to_dict()

                # iterate back one day
                iterables["load_date"] = iterables["load_date"] - datetime.timedelta(days=1)
                iterables["two_year_transaction_period"] = current_cycle
                iterables["complete"] = False

            logger.info(' - '.join(['INFO', 'loading new date', iterables["load_date"].strftime('%Y-%m-%d'), str(iterables["two_year_transaction_period"])]))

    return iterables

# gets schedule a data from FEC API and load into Elasticsearch
def federal_fec_ingest_get_receipts(message, context):

    # get iterables
    iterables = get_iterables()
    logger.info(' - '.join(['INFO', 'starting load', iterables["load_date"].strftime('%Y-%m-%d'), str(iterables["two_year_transaction_period"])]))

    # loop through documents
    start_time = time.time()
    while time.time() - start_time < 520:

        # get new results
        pagination = loop(iterables["two_year_transaction_period"], iterables["load_date"], iterables["pagination"]["last_indexes"]["last_contribution_receipt_amount"], iterables["pagination"]["last_indexes"]["last_index"])
        iterables["last_updated"] = datetime.datetime.now()

        # if the count changes because we are in the middle of an update, restart the pagination for this load_date and cycle combination
        if iterables["load_date"].strftime('%Y-%m-%d') == datetime.datetime.now(pytz.timezone('EST')).strftime('%Y-%m-%d') and iterables["pagination"]["count"] is not None and pagination["count"] != iterables["pagination"]["count"]:
            logger.info(' - '.join(['INFO', 'restarting due to count change', iterables["load_date"].strftime('%Y-%m-%d'), str(iterables["two_year_transaction_period"])]))
            iterables = get_iterables(iterables=iterables, reset=True)
        else:
            # if we are done paging, get new iterables
            if pagination["last_indexes"] is None:
                logger.info(' - '.join(['INFO', 'done paging', iterables["load_date"].strftime('%Y-%m-%d'), str(iterables["two_year_transaction_period"])]))
                iterables = get_iterables(iterables=iterables, advance=True)
            # otherwise, update the pagination
            else:
                iterables["pagination"] = pagination

    # update iterables in firestore
    ref.document(iterables["load_date"].strftime('%Y-%m-%d')).set(iterables, merge=True)
    logger.info(' - '.join(['INFO', 'firestore updated', iterables["load_date"].strftime('%Y-%m-%d'), str(iterables["two_year_transaction_period"])]))

    return True
