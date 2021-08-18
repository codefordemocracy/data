import csv
import pytz
import datetime
import json
import logging
import os
import time
from io import StringIO

import requests
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
from google.cloud import firestore, secretmanager, storage
from irsx.xmlrunner import XMLRunner
from irsx.filing import InvalidXMLException

import utilities

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
elastic_host = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_host/versions/1"}).payload.data.decode()
elastic_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_username_data/versions/1"}).payload.data.decode()
elastic_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_password_data/versions/1"}).payload.data.decode()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme='https', port=443)
db = firestore.Client()
client = storage.Client()

# indexes IRS 990s into ElasticSearch
def federal_irs_ingest_get_990s(message, context):

    year = datetime.datetime.today().year

    # settings pulled from a database
    ref = db.collection('federal').document('irs').collection('990s').document(str(year))
    settings = ref.get().to_dict()
    if settings is not None:
        latest_saved_idx = settings['idx']
    else:
        latest_saved_idx = 0

    # prep load
    xml_runner = XMLRunner()
    start_time = time.time()
    bucket = client.get_bucket(gcp_project_id)
    blob = bucket.get_blob('downloads/federal/irs/index_' + str(year) + '.csv')
    blob = blob.download_as_string().decode('utf-8')
    blob = StringIO(blob)

    # load by looping through all the rows in the index
    actions = []
    failed_object_ids = []
    reader = csv.reader(blob, delimiter=',')
    next(reader) # skip header
    for idx, row in enumerate(reader):

        if time.time() - start_time > 520:
            break

        # skip previously indexed objectss
        if idx < latest_saved_idx:
            continue

        # process the object id
        object_id = row[8]
        if int(object_id[:4]) < 2014: # can't process these
            continue

        # process the submission date
        sub_date = row[4]
        try:
            sub_date = datetime.datetime.strptime(sub_date, '%m/%d/%Y %I:%M:%S %p')
        except:
            try:
                sub_date = datetime.datetime.strptime(sub_date, '%m/%d/%Y')
            except:
                raise

        sub_date = pytz.timezone('US/Eastern').localize(sub_date)
        sub_date = sub_date.strftime("%Y-%m-%dT%H:%M:%S%z")

        # grab the filing
        try:
            filing = xml_runner.run_filing(object_id)
            schedules = filing.get_result()
        except (RuntimeError, InvalidXMLException) as e:
            logger.error(object_id, str(e))
            failed_object_ids.append(object_id)
            continue

        if schedules is not None:

            xml = utilities.get_xml_parts(schedules)
            xml = utilities.clean_xml(xml)

            if 'IRS990EZ' in xml:
                index = '990ez'
            elif 'IRS990PF' in xml:
                index = '990pf'
            else:
                index = '990'

            actions.append({
                '_op_type': 'index',
                '_index': 'federal_irs_' + index,
                '_id': object_id,
                '_source': {
                    'row': {
                        'return_id': str(row[0]),
                        'filing_type': row[1],
                        'ein': str(row[2]),
                        'tax_period': row[3],
                        'sub_date': sub_date,
                        'taxpayer_name': row[5],
                        'return_type': str(row[6]),
                        'dln': str(row[7]),
                        'object_id': object_id
                    },
                    'obj': xml,
                    'context': {
                        'last_indexed': datetime.datetime.now(datetime.timezone.utc)
                    }
                }
            })

        if len(actions) >= 1000:
            helpers.bulk(es, actions)
            logger.info('ELASTICSEARCH UPDATED' + ' - ' + str(len(actions)) + ' docs')
            actions = []

    # index all docs into elasticsearch
    if actions:
        helpers.bulk(es, actions)
        logger.info('ELASTICSEARCH UPDATED' + ' - ' + str(len(actions)) + ' docs')

    # update Firestore
    update = {
        "idx": idx,
        "last_updated": datetime.datetime.now(datetime.timezone.utc)
    }
    if len(failed_object_ids) > 0:
        update['failed_object_ids'] = firestore.ArrayUnion(failed_object_ids)
    ref.set(update, merge=True)

    num_remaining_rows = len(list(reader))
    logger.info('FIRESTORE UPDATED - completed: ' + str(idx) + ', remaining: ' + str(num_remaining_rows))
    return num_remaining_rows
