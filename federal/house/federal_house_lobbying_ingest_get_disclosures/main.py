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
elastic_host = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_host/versions/1"}).payload.data.decode()
elastic_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_username_data/versions/1"}).payload.data.decode()
elastic_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_password_data/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
db = firestore.Client()

# other settings
firestore_idx_name = 'current-year-idx'
# firestore_idx_name = '2008-2020-idx'
# firestore_idx_name = '2000-2007-idx'
house_api_url = 'https://clerkapi.house.gov/Elastic/search'
xml_report_base_url = 'https://disclosurespreview.house.gov/ld/ldxmlrelease/{report_year}/{report_type_code}/{_id}.xml'

# helper function to generate url to xml
def url_from_hit(hit):
    return xml_report_base_url.format(report_year=hit['reportYear'], report_type_code=hit['reportTypeCode'], _id=hit['_id'])

# helper function to generate json for the request for the house api
def json_data_builder(year, report_type, order, from_):
    return json.dumps(
        {
            'index': 'lobbying-disclosures',
            'aggregations': [
                {
                    'name': 'Filing Year',
                    'field': 'reportYear',
                    'sort': 'desc',
                    'filterable': False,
                    'scrolling': False,
                    'position': 1
                },
                {
                    'name': 'Report Type',
                    'field': 'reportType.description',
                    'sort': 'asc',
                    'filterable': False,
                    'scrolling': True,
                    'toggled': True,
                    'position': 2
                },
                {
                    'name': 'Issue Areas',
                    'field': 'issueAreaCodes.description',
                    'sort': 'asc',
                    'filterable': False,
                    'scrolling': True,
                    'toggled': True,
                    'position': 3,
                    'subFields': ['issueAreaCodes.name']
                },
                {
                    'name': 'Conviction',
                    'field': 'convictionDisclosure.convictionDisclosureIndicator',
                    'sort': 'desc',
                    'filterable': False,
                    'scrolling': False,
                    'type': 'boolean',
                    'position': 4
                },
                {
                    'name': "Client's Country",
                    'field': 'client.address.country',
                    'sort': 'asc',
                    'filterable': False,
                    'scrolling': True,
                    'toggled': True,
                    'position': 6
                },
                {
                    'name': "Client's State/Province",
                    'field': 'client.address.state',
                    'sort': 'asc',
                    'filterable': True,
                    'scrolling': True,
                    'toggled': True,
                    'position': 5
                },
                {
                    'name': "Foreign Entity's Country",
                    'field': 'foreignEntities.address.country',
                    'sort': 'asc',
                    'filterable': False,
                    'scrolling': True,
                    'toggled': True,
                    'position': 7
                }
            ],
             'size': 100,
             'from': from_,
             'sort': [{'_score': True}, {'field': 'client.name', 'order': order}],
             'filters': {"reportYear": [str(year)], "reportType.description": [report_type]},
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

# indexes House lobbying disclosures into ElasticSearch
def federal_house_lobbying_ingest_get_disclosures(message, context):

    # settings pulled from a database
    ref = db.collection('federal').document('house').collection('lobbying').document('disclosures')
    settings = ref.get().to_dict()
    previously_firestore_saved_options_idx = settings[firestore_idx_name]

    # other settings
    years = [datetime.datetime.today().year]
    # years = list(range(2008, 2021))
    # years = list(range(2000, 2008))
    report_types = [
        "1st Quarter Amendment Report", "1st Quarter Report", "1st Quarter Termination Amendment Report", "1st Quarter Termination Report",
        "2nd Quarter Amendment Report", "2nd Quarter Report", "2nd Quarter Termination Amendment Report", "2nd Quarter Termination Report",
        "3rd Quarter Amendment Report", "3rd Quarter Report", "3rd Quarter Termination Amendment Report", "3rd Quarter Termination Report",
        "4th Quarter Amendment Report", "4th Quarter Report", "4th Quarter Termination Amendment Report", "4th Quarter Termination Report",
        "Registration", "Registration Amendment",
    ]
    # report_types = [
    #      "Registration", "Registration Amendment",
    #      "Mid-Year Amendment Report", "Mid-Year Report", "Mid-Year Termination Amendment Report", "Mid-Year Termination Report",
    #      "Year-End Amendment Report", "Year-End Report", "Year-End Termination Amendment Report", "Year-End Termination Report"
    # ] # for 2000-2007 historical load
    orders = ["asc", "desc"]
    froms = list(range(0, 9900, 100))
    options = []
    for y in years:
        for r in report_types:
            for o in orders:
                for f in froms:
                    options.append((y, r, o, f))
    headers = {
        'Host': 'clerkapi.house.gov',
        'Connection': 'keep-alive',
        'Content-Length': '1242',
        'sec-ch-ua': '"Google Chrome";v="89", "Chromium";v="89", ";Not A Brand";v="99"',
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
    continue_until_report_type_changes = None

    # restart if at end
    if len(options) - 1 == previously_firestore_saved_options_idx:
        previously_firestore_saved_options_idx = 0

    # load by looping through all the possibilities
    for idx, option in enumerate(options):

        # skip previously tried options
        if idx < previously_firestore_saved_options_idx:
            continue

        # skip options if we already know there will be no results
        if continue_until_report_type_changes == option[1]:
            continue
        else:
            continue_until_report_type_changes = None

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
            file_url = url_from_hit(hit)
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
            if "LOBBYINGDISCLOSURE2" in dict_from_xml:
                json_from_dict = json.loads(json.dumps(dict_from_xml["LOBBYINGDISCLOSURE2"]))
            else:
                json_from_dict = json.loads(json.dumps(dict_from_xml["LOBBYINGDISCLOSURE1"]))
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
            if json_from_dict.get("effectiveDate") is not None:
                try:
                    json_from_dict["effectiveDate"] = datetime.datetime.strptime(json_from_dict["effectiveDate"], '%m/%d/%Y %I:%M:%S %p')
                except:
                    try:
                        json_from_dict["effectiveDate"] = datetime.datetime.strptime(json_from_dict["effectiveDate"], '%m/%d/%Y')
                    except:
                        try:
                            json_from_dict["effectiveDate"] = datetime.datetime.strptime(json_from_dict["effectiveDate"], '%m/%d/%y')
                        except:
                            try:
                                json_from_dict["effectiveDate"] = datetime.datetime.strptime(json_from_dict["effectiveDate"], '%m-%d-%Y')
                            except:
                                try:
                                    json_from_dict["effectiveDate"] = datetime.datetime.strptime(json_from_dict["effectiveDate"], '%m-%d-%y')
                                except:
                                    try:
                                        json_from_dict["effectiveDate"] = datetime.datetime.strptime(json_from_dict["effectiveDate"], '%m.%d.%Y')
                                    except:
                                        try:
                                            json_from_dict["effectiveDate"] = datetime.datetime.strptime(json_from_dict["effectiveDate"], '%m.%d.%y')
                                        except:
                                            try:
                                                json_from_dict["effectiveDate"] = datetime.datetime.strptime(json_from_dict["effectiveDate"], '%m%d%Y')
                                            except:
                                                try:
                                                    json_from_dict["effectiveDate"] = datetime.datetime.strptime(json_from_dict["effectiveDate"], '%m%d%y')
                                                except:
                                                    raise
                json_from_dict["effectiveDate"] = pytz.timezone('US/Eastern').localize(json_from_dict["effectiveDate"])
                json_from_dict["effectiveDate"] = json_from_dict["effectiveDate"].strftime("%Y-%m-%dT%H:%M:%S%z")
            if json_from_dict.get("terminationDate") is not None:
                try:
                    json_from_dict["terminationDate"] = datetime.datetime.strptime(json_from_dict["terminationDate"], '%m/%d/%Y %I:%M:%S %p')
                except:
                    try:
                        json_from_dict["terminationDate"] = datetime.datetime.strptime(json_from_dict["terminationDate"], '%m/%d/%Y')
                    except:
                        try:
                            json_from_dict["terminationDate"] = datetime.datetime.strptime(json_from_dict["terminationDate"], '%m/%d/%y')
                        except:
                            try:
                                json_from_dict["terminationDate"] = datetime.datetime.strptime(json_from_dict["terminationDate"], '%m-%d-%Y')
                            except:
                                try:
                                    json_from_dict["terminationDate"] = datetime.datetime.strptime(json_from_dict["terminationDate"], '%m-%d-%y')
                                except:
                                    try:
                                        json_from_dict["terminationDate"] = datetime.datetime.strptime(json_from_dict["terminationDate"], '%m.%d.%Y')
                                    except:
                                        try:
                                            json_from_dict["terminationDate"] = datetime.datetime.strptime(json_from_dict["terminationDate"], '%m.%d.%y')
                                        except:
                                            try:
                                                json_from_dict["terminationDate"] = datetime.datetime.strptime(json_from_dict["terminationDate"], '%m%d%Y')
                                            except:
                                                try:
                                                    json_from_dict["terminationDate"] = datetime.datetime.strptime(json_from_dict["terminationDate"], '%m%d%y')
                                                except:
                                                    raise
                json_from_dict["terminationDate"] = pytz.timezone('US/Eastern').localize(json_from_dict["terminationDate"])
                json_from_dict["terminationDate"] = json_from_dict["terminationDate"].strftime("%Y-%m-%dT%H:%M:%S%z")
            if json_from_dict.get("alis", {}).get("ali_info") is not None:
                if not isinstance(json_from_dict["alis"]["ali_info"], list):
                    json_from_dict["alis"]["ali_info"] = [json_from_dict["alis"]["ali_info"]]
                for i in json_from_dict["alis"]["ali_info"]:
                    if i.get("federal_agencies") is not None:
                        i["federal_agencies"] = json.dumps(i["federal_agencies"])
            processed = {
                "date_submitted": json_from_dict.get("signedDate"),
                "effective_date": json_from_dict.get("effectiveDate"),
                "termination_date": json_from_dict.get("terminationDate"),
                "filing_year": int(json_from_dict.get("reportYear")),
                "filing_type": json_from_dict.get("reportType"),
                "client": {
                    "name": json_from_dict.get("clientName"),
                    "description": json_from_dict.get("clientGeneralDescription"),
                    "country": json_from_dict.get("clientCountry"),
                    "state": json_from_dict.get("clientState"),
                    "senate_id": json_from_dict.get("senateID").split("-")[1] if "-" in json_from_dict.get("senateID") else json_from_dict.get("senateID"),
                },
                "registrant": {
                    "name": json_from_dict.get("organizationName"),
                    "description": json_from_dict.get("registrantGeneralDescription"),
                    "country": json_from_dict.get("country"),
                    "state": json_from_dict.get("state"),
                    "senate_id": json_from_dict.get("senateID").split("-")[0] if "-" in json_from_dict.get("senateID") else json_from_dict.get("senateID"),
                    "house_id": json_from_dict.get("houseID"),
                    "contact": json_from_dict.get("printedName"),
                },
                "url": "https://disclosurespreview.house.gov/ld/ldxmlrelease/" + json_from_dict.get("reportYear") + "/" + json_from_dict.get("reportType") + "/" + hit["_id"] + ".xml"
            }
            issues = []
            activities = []
            lobbyists = []
            coverage = []
            if json_from_dict.get("alis", {}).get("ali_Code") is not None:
                issues = [{"code": c} for c in json_from_dict["alis"]["ali_Code"] if c is not None]
            if json_from_dict.get("alis", {}).get("ali_info") is not None:
                for i in json_from_dict.get("alis", {}).get("ali_info"):
                    if i.get("issueAreaCode") is not None:
                        issues.append({"code": i.get("issueAreaCode")})
                    if i.get("specific_issues", {}).get("description") is not None:
                        activities.append(i.get("specific_issues", {}).get("description"))
            if json_from_dict.get("specific_issues") is not None:
                activities.append(json_from_dict["specific_issues"])
            if json_from_dict.get("lobbyists", {}).get("lobbyist") is not None:
                for lob in json_from_dict["lobbyists"]["lobbyist"]:
                    name = [lob.get("lobbyistFirstName"), lob.get("lobbyistLastName"), lob.get("lobbyistSuffix")]
                    name = [n for n in name if n is not None]
                    if len(name) > 0:
                        lobbyists.append({
                            "name": " ".join(name)
                        })
                    if lob.get("coveredPosition") is not None:
                        coverage.append(lob.get("coveredPosition"))
            if len(issues) > 0:
                processed["issues"] = issues
            if len(activities) > 0:
                processed["activities"] = activities
            if len(lobbyists) > 0:
                processed["lobbyists"] = lobbyists
            if len(coverage) > 0:
                processed["coverage"] = coverage
            actions.append(
                {
                    '_op_type': 'index',
                    '_index': 'federal_house_lobbying_disclosures',
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
            continue_until_report_type_changes = option[1]

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
