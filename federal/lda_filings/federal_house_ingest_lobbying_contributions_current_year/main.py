import datetime
import json
import logging
import time

import requests
import xmltodict
from google.cloud import firestore
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, helpers

db = firestore.Client()
ref = db.collection('house').document('lobbying-contributions')
settings = ref.get().to_dict()
firestore_idx_name = 'current-year-idx'

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
elastic_host = secrets.access_secret_version(request={'name': 'projects/952416783871/secrets/elastic_host/versions/1'}).payload.data.decode()
elastic_username_data = secrets.access_secret_version(request={'name': 'projects/952416783871/secrets/elastic_username_data/versions/1'}).payload.data.decode()
elastic_password_data = secrets.access_secret_version(request={'name': 'projects/952416783871/secrets/elastic_password_data/versions/1'}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme='https', port=443)
es.indices.refresh()

house_api_url = 'https://clerkapi.house.gov/Elastic/search'
xml_report_base_url = 'http://disclosurespreview.house.gov/lc/lcxmlrelease/{report_year}/{report_type_code}/{_id}.xml'

years = [(datetime.datetime.today().year]
report_types = ['Mid-Year Report', 'Year-End Report']
amendments = ['true', 'false']
filer_types = ['L', 'O']
orders = ['asc', 'desc']
froms = list(range(0, 9900, 100))

options = []
for y in years:
    for rt in report_types:
        for a in amendments:
            for ft in filer_types:
                for o in orders:
                    for f in froms:
                        options.append((y, rt, a, ft, o, f))

headers = {
    'Host': 'clerkapi.house.gov',
    'Connection': 'keep-alive',
    'Content-Length': '790',
    'sec-ch-ua': "'Google Chrome';v='89', 'Chromium';v='89', ';Not A Brand';v='99'",
    'DNT': '1',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36',
    'Content-Type': 'application/json',
    'Accept': '*/*',
    'Origin': 'https://disclosurespreview.house.gov',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,ja-JP;q=0.8,ja;q=0.7,es;q=0.6'
}

failed_urls = []

def url_from_hit(hit, amendment):
    report_type_code = hit['reportTypeCode']['name'] if amendment == 'false' else 'MA'
    return xml_report_base_url.format(report_year=hit['reportYear'], report_type_code=report_type_code, _id=hit['_id'])

def json_data_builder(year, report_type, amendment, filer_type, order, from_):
    return json.dumps(
        {
            'index': 'lobbying-contributions',
            'aggregations': [
                {
                    'name': 'Filing Year',
                    'field': 'reportYear',
                    'sort': 'desc',
                    'filterable': False,
                    'scrolling': False,
                    'position':1
                },
                {
                    'name': 'Report Type',
                    'field': 'reportType.description',
                    'sort': 'asc',
                    'filterable': False,
                    'scrolling': True,
                    'position': 2
                },
                {
                    'name': 'Amendment',
                    'field': 'amendment',
                    'sort': 'desc',
                    'filterable': False,
                    'scrolling': False,
                    'type': 'boolean', 
                    'position': 3
                },
                {
                    'name': 'Filer Type',
                    'field': 'filerType',
                    'sort': 'asc',
                    'filterable': False,
                    'scrolling': False,
                    'position':4
                }
            ],
                'size': 100,
                'from': from_,
                'sort': [{'_score': True}, {'field': 'reportType.name', 'order': order}],
                'filters': ({'reportType.description': [report_type], 'amendment': [amendment], 'filerType':[filer_type], 'reportYear': [str(year)]}),
                'keyword': '',
                'matches': [],
                'ranges': [],
                'applicationName': 'disclosures.house.gov'
        }
    )

def preprocess_xml(raw_xml):
    for idx, ch in enumerate(raw_xml):
        if ch == '<':
            break
    return raw_xml[idx:]

def get_xml_file_text(url):
    r = requests.get(url, timeout=5)
    if r.status_code != 200:
        return
    return r.text

def federal_house_ingest_lobbying_contributions_current_year(message, context):
    previously_firestore_saved_options_idx = settings[firestore_idx_name]
    exit = False
    start_time = time.time()
    continue_until_filer_type_changes = None
    if len(options) - 1 == previously_firestore_saved_options_idx:
        previously_firestore_saved_options_idx = 0
    for idx, option in enumerate(options):
        if idx < previously_firestore_saved_options_idx:
            continue
        if continue_until_filer_type_changes == option[3]:
            continue
        else:
            continue_until_filer_type_changes = None
        json_data = json_data_builder(*option)
        r = requests.post(url=house_api_url, headers=headers, data=json_data, verify=False, timeout=5)
        hits = r.json().get('filteredHits')
        if r.status_code != 200:
            break
        actions = []
        for hit in hits:
            if time.time() - start_time > 520:
                exit = True
                break
            file_url = url_from_hit(hit, option[2])
            xml_file_text = get_xml_file_text(file_url)
            if not xml_file_text:
                failed_urls.append(file_url)
                continue
            processed_xml_file_text = preprocess_xml(xml_file_text)
            dict_from_xml = xmltodict.parse(processed_xml_file_text)
            json_from_dict = json.dumps(dict_from_xml)
            actions.append(
                {
                    '_op_type': 'index',
                    '_index': 'house-lobbying-contributions',
                    '_id': hit['_id'],
                    '_source': json_from_dict
                }
            )
        if actions:
            helpers.bulk(es, actions)
            logger.info('ELASTICSEARCH UPDATED')
        if len(hits) < 100:
            continue_until_filer_type_changes = option[3]
        if exit:
            break

    # update Firestore
    settings[firestore_idx_name] = idx
    settings[firestore_idx_name + '-last-updated'] = datetime.datetime.now(datetime.timezone.utc)
    settings[firestore_idx_name + '-failed-urls'] = failed_urls
    ref.set(settings)
    logger.info('FIRESTORE UPDATED; new idx: ' + str(idx) + '/' + str(len(options) - 1))
    return True
