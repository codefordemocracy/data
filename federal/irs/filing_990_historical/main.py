import csv
import datetime
import json
import logging
import os
import time

from elasticsearch import Elasticsearch, helpers
from google.cloud import firestore, secretmanager
from irsx.xmlrunner import XMLRunner
from irsx.filing import InvalidXMLException

from filing_parsers_990pf import grants_from_990pf, org_from_990pf, timestamp_now
from filing_parsers_990 import grants_from_990, org_from_990
from filing_parsers_990ez import org_from_990ez # 990EZ filers don't have grants


# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)

# get secrets   
secrets = secretmanager.SecretManagerServiceClient()
elastic_host = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_host/versions/1"}).payload.data.decode()
elastic_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_username_data/versions/1"}).payload.data.decode()
elastic_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_password_data/versions/1"}).payload.data.decode()

es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme='https', port=443)
es.indices.refresh()

db = firestore.Client()
ref = db.collection('irs').document('990-filings-historical')
settings = ref.get().to_dict()


def filing_990_historical(message, context):
    latest_saved_year = settings['latest_year_file']
    latest_saved_idx = settings['latest_index_in_file']
    failed_object_ids = settings['failed_object_ids']
    if latest_saved_year == 2010:
        return True
    xml_runner = XMLRunner()
    start_time = time.time()
    exit = False
    files = os.listdir('indexes')
    actions = []
    for _file in files:
        if _file != str(latest_saved_year) + '.csv':
            continue
        with open('indexes/' + _file, newline='\n') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            next(reader) # skip header
            for idx, row in enumerate(reader):
                if time.time() - start_time > 520:
                    exit = True
                    break
                if idx < latest_saved_idx:
                    continue
                object_id = row[-1]
                try:
                    filing = xml_runner.run_filing(object_id)
                except (RuntimeError, InvalidXMLException) as e:
                    failed_object_ids.append(object_id)
                    continue
                try:
                    schedules = filing.list_schedules()
                    if 'IRS990PF' in schedules:
                        org = org_from_990pf(filing)
                        grants_to_create = grants_from_990pf(filing)
                    elif 'IRS990EZ' in schedules:
                        org = org_from_990ez(filing)
                        grants_to_create = []
                    elif 'IRS990' in schedules:
                        org = org_from_990(filing)
                        grants_to_create = grants_from_990(filing)
                    else:
                        raise RuntimeError('No schedule available to parse.')
                except (RuntimeError, Exception) as e:
                    failed_object_ids.append(object_id)
                    continue
                actions.append({
                    '_op_type': 'index',
                    '_index': 'irs-990-filing',
                    '_id': object_id,
                    '_source':  json.dumps({'org': org, 'grants': grants_to_create})
                })
            else:
                latest_saved_year -= 1
        if exit:
            break
    if actions:
        helpers.bulk(es, actions)
        actions = []
        logger.info('ELASTICSEARCH UPDATED')
    settings['latest_year_file'] = latest_saved_year
    settings['latest_index_in_file'] = idx
    settings['failed_object_ids'] = failed_object_ids
    ref.set(settings)
    logger.info('FIRESTORE UPDATED')
    return True
