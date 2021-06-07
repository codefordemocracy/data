import datetime
import pytz
import json
import logging
import time

import requests
import urllib3
import xmltodict
from google.cloud import firestore
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, helpers

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
db = firestore.Client()

# other settings
firestore_idx_name = 'current-year-idx'
# firestore_idx_name = 'historical-idx'
house_api_url = 'https://clerkapi.house.gov/Elastic/search'
xml_report_base_url = 'https://disclosurespreview.house.gov/lc/lcxmlrelease/{report_year}/{report_type_code}/{_id}.xml'

# helper function to generate url to xml
def url_from_hit(hit, amendment):
    report_type_code = hit['reportTypeCode']['name'] if amendment == 'false' else 'MA'
    return xml_report_base_url.format(report_year=hit['reportYear'], report_type_code=report_type_code, _id=hit['_id'])

# helper function to generate json for the request for the house api
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

# indexes House lobbying contributions into ElasticSearch
def federal_house_lobbying_ingest_get_contributions(message, context):

    # settings pulled from a database
    ref = db.collection('federal').document('house').collection('lobbying').document('contributions')
    settings = ref.get().to_dict()
    previously_firestore_saved_options_idx = settings[firestore_idx_name]

    # other settings
    years = [datetime.datetime.today().year]
    # years = list(range(2008, 2021))
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

    # prep load
    failed_urls = []
    exit = False
    start_time = time.time()
    continue_until_filer_type_changes = None

    # restart if at end
    if len(options) - 1 == previously_firestore_saved_options_idx:
        previously_firestore_saved_options_idx = 0

    # load by looping through all the possibilities
    for idx, option in enumerate(options):

        # skip previously tried options
        if idx < previously_firestore_saved_options_idx:
            continue

        # skip options if we already know there will be no results
        if continue_until_filer_type_changes == option[3]:
            continue
        else:
            continue_until_filer_type_changes = None

        # get the rows from the table
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

            # process each xml file
            file_url = url_from_hit(hit, option[2])
            xml_file_text = get_xml_file_text(file_url)
            if not xml_file_text:
                failed_urls.append(file_url)
                continue
            processed_xml_file_text = preprocess_xml(xml_file_text)
            try:
                dict_from_xml = xmltodict.parse(processed_xml_file_text)
            except:
                failed_urls.append(file_url)
                continue
            json_from_dict = json.loads(json.dumps(dict_from_xml["CONTRIBUTIONDISCLOSURE"]))
            if json_from_dict.get("signedDate") is not None:
                try:
                    json_from_dict["signedDate"] = datetime.datetime.strptime(json_from_dict["signedDate"], '%m/%d/%Y %I:%M:%S %p')
                except:
                    try:
                        json_from_dict["signedDate"] = datetime.datetime.strptime(json_from_dict["signedDate"], '%m/%d/%Y')
                    except:
                        raise
                json_from_dict["signedDate"] = pytz.timezone('US/Eastern').localize(json_from_dict["signedDate"])
                json_from_dict["signedDate"] = json_from_dict["signedDate"].strftime("%Y-%m-%dT%H:%M:%S%z")
            if json_from_dict.get("pacs") is not None:
                if json_from_dict.get("pacs", {}).get("pac") is not None:
                    if not isinstance(json_from_dict["pacs"]["pac"], list):
                        json_from_dict["pacs"]["pac"] = [json_from_dict["pacs"]["pac"]]
                if json_from_dict.get("pacs", {}).get("name") is not None:
                    if json_from_dict["pacs"]["pac"] is not None:
                        json_from_dict["pacs"]["pac"].append({"name": json_from_dict["pacs"].pop("name")})
                    else:
                        json_from_dict["pacs"]["pac"] = [{"name": json_from_dict["pacs"].pop("name")}]
            if json_from_dict.get("contributions") is not None:
                if json_from_dict.get("contributions", {}).get("contribution") is not None:
                    if not isinstance(json_from_dict["contributions"]["contribution"], list):
                        json_from_dict["contributions"]["contribution"] = [json_from_dict["contributions"]["contribution"]]
            processed = {
                "date_submitted": json_from_dict.get("signedDate"),
                "filing_year": int(json_from_dict.get("reportYear")),
                "filing_type": json_from_dict.get("reportType"),
                "registrant": {
                    "name": json_from_dict.get("organizationName"),
                    "country": json_from_dict.get("country"),
                    "state": json_from_dict.get("state"),
                    "senate_id": json_from_dict.get("senateRegID"),
                    "house_id": json_from_dict.get("houseRegID"),
                    "contact": json_from_dict.get("contactName"),
                },
                "no_contributions": json_from_dict.get("noContributions"),
                "url": "https://disclosurespreview.house.gov/lc/lcxmlrelease/" + json_from_dict.get("reportYear") + "/" + json_from_dict.get("reportType") + "/" + hit["_id"] + ".xml"
            }
            if json_from_dict.get("pacs") is not None:
                pacs = [p.get("name") for p in json_from_dict.get("pacs", {}).get("pac", []) if p is not None]
                pacs = [p for p in pacs if p is not None]
                if len(pacs) > 0:
                    processed["pacs"] = ", ".join(pacs)
            lobbyist = {}
            if json_from_dict.get("lobbyistID") is not None:
                lobbyist["id"] = json_from_dict.get("lobbyistID")
            name = [json_from_dict.get("lobbyistFirstName"), json_from_dict.get("lobbyistMiddleName"), json_from_dict.get("lobbyistLastName"), json_from_dict.get("lobbyistSuffix")]
            name = [n for n in name if n is not None]
            if len(name) > 0:
                lobbyist["name"] = " ".join(name)
            if len(lobbyist) > 0:
                processed["lobbyist"] = lobbyist
            contributions = []
            if json_from_dict.get("contributions") is not None:
                if json_from_dict.get("contributions", {}).get("contribution") is not None:
                    for c in json_from_dict.get("contributions", {}).get("contribution"):
                        dt = c.get("date")
                        if dt == "02/31/2008":
                            dt = "02/29/2008"
                        if dt is not None:
                            try:
                                dt = datetime.datetime.strptime(dt, '%m/%d/%Y')
                            except:
                                try:
                                    dt = datetime.datetime.strptime(dt, '%m/%d/%y')
                                except:
                                    try:
                                        dt = datetime.datetime.strptime(dt, '%m-%d-%Y')
                                    except:
                                        try:
                                            dt = datetime.datetime.strptime(dt, '%m-%d-%y')
                                        except:
                                            try:
                                                dt = datetime.datetime.strptime(dt, '%m.%d.%Y')
                                            except:
                                                try:
                                                    dt = datetime.datetime.strptime(dt, '%m.%d.%y')
                                                except:
                                                    try:
                                                        dt = datetime.datetime.strptime(dt, '%m%d%Y')
                                                    except:
                                                        try:
                                                            dt = datetime.datetime.strptime(dt, '%m%d%y')
                                                        except:
                                                            try:
                                                                dt = datetime.datetime.strptime(dt, '%m/%d %Y')
                                                            except:
                                                                continue
                            dt = pytz.timezone('US/Eastern').localize(dt)
                            dt = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                        if c.get("type") is not None or c.get("contributorName") is not None or c.get("payeeName") is not None or c.get("recipientName") is not None or c.get("amount") is not None or dt is not None:
                            contributions.append({
                                "contribution_type": c.get("type"),
                                "contributor_name": c.get("contributorName"),
                                "payee_name": c.get("payeeName"),
                                "recipient_name": c.get("recipientName"),
                                "amount": float(c.get("amount").replace(',', '').replace('$', '').replace(' ', '')) if c.get("amount") is not None else None,
                                "date": dt
                            })
            if len(contributions) > 0:
                processed["contributions"] = contributions
            actions.append(
                {
                    '_op_type': 'index',
                    '_index': 'federal_house_lobbying_contributions',
                    '_id': hit['_id'],
                    '_source': {
                        'obj': json_from_dict,
                        'processed': processed,
                        'meta': {
                            'last_indexed': datetime.datetime.now(datetime.timezone.utc)
                        }
                    }
                }
            )

        if actions:
            helpers.bulk(es, actions)
            logger.info('ELASTICSEARCH UPDATED' + ' - ' + str(len(actions)) + ' docs')

        if len(hits) < 100:
            continue_until_filer_type_changes = option[3]

        if exit:
            break

    # update Firestore
    update = {
        firestore_idx_name: idx,
        firestore_idx_name + '-last-updated': datetime.datetime.now(datetime.timezone.utc)
    }
    if len(failed_urls) > 0:
        update[firestore_idx_name + '-failed-urls'] = firestore.ArrayUnion(failed_urls)
    ref.set(update, merge=True)
    logger.info('FIRESTORE UPDATED - new idx: ' + str(idx) + '/' + str(len(options) - 1))
    return True
