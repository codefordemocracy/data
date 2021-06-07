import time
import datetime
import logging

import requests
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, helpers
import datetime

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
elastic_host = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_host/versions/1"}).payload.data.decode()
elastic_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_username_data/versions/1"}).payload.data.decode()
elastic_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_password_data/versions/1"}).payload.data.decode()
federal_senate_lobbying_api_key = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/federal_senate_lobbying_api_key/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)

# other settings
index = 'federal_senate_lobbying_disclosures'

# helper loop
def loop(headers, url):
    actions = []
    r = requests.get(url, headers=headers)
    for filing in r.json()['results']:
        key = filing.get('filing_uuid')
        processed = {
            "date_submitted": filing.get("dt_posted"),
            "effective_date": filing.get("client", {}).get("effective_date"),
            "termination_date": filing.get("termination_date"),
            "filing_year": filing.get("filing_year"),
            "filing_type": filing.get("filing_type"),
            "client": {
                "name": filing.get("client", {}).get("name"),
                "description": filing.get("client", {}).get("general_description"),
                "country": filing.get("client", {}).get("country"),
                "state": filing.get("client", {}).get("state"),
                "senate_id": str(filing.get("client", {}).get("id")),
            },
            "registrant": {
                "name": filing.get("registrant", {}).get("name"),
                "description": filing.get("registrant", {}).get("description"),
                "country": filing.get("registrant", {}).get("country"),
                "state": filing.get("registrant", {}).get("state"),
                "senate_id": str(filing.get("registrant", {}).get("id")),
                "house_id": str(filing.get("registrant", {}).get("house_registrant_id")),
                "contact": filing.get("registrant", {}).get("contact_name"),
            },
            "url": filing.get("filing_document_url")
        }
        issues = []
        activities = []
        lobbyists = []
        coverage = []
        for act in filing.get("lobbying_activities"):
            if act.get("general_issue_code") is not None:
                issues.append({
                    "code": act.get("general_issue_code"),
                    "display": act.get("general_issue_code_display")
                })
            if act.get("description") is not None:
                activities.append(act.get("description"))
            if act.get("lobbyists") is not None:
                for lob in act.get("lobbyists"):
                    if lob.get("lobbyist") is not None:
                        name = [lob["lobbyist"].get("first_name"), lob["lobbyist"].get("middle_name"), lob["lobbyist"].get("last_name"), lob["lobbyist"].get("suffix")]
                        name = [n for n in name if n is not None]
                        if len(name) > 0:
                            lobbyists.append({
                                "id": lob["lobbyist"].get("id"),
                                "name": " ".join(name)
                            })
                    if lob.get("covered_position") is not None:
                        if lob.get("covered_position") != 'N/A':
                            coverage.append(lob.get("covered_position"))
        if len(issues) > 0:
            processed["issues"] = issues
        if len(activities) > 0:
            processed["activities"] = activities
        if len(lobbyists) > 0:
            processed["lobbyists"] = lobbyists
        if len(coverage) > 0:
            processed["coverage"] = coverage
        actions.append({
            "_op_type": "index",
            "_index": index,
            "_id": key,
            "_source": {
                "obj": filing,
                "processed": processed,
                "meta": {
                    "last_indexed": datetime.datetime.now(datetime.timezone.utc)
                }
            }
        })
    helpers.bulk(es, actions)
    return r.json().get('next')

# indexes Senate lobbying disclosures into ElasticSearch
def federal_senate_lobbying_ingest_get_disclosures(message, context):
    es.indices.refresh()
    last_saved_count = int(es.cat.count(index, params={"format": "json"})[0]['count'])
    next_page = None
    start_time = time.time()
    last_saved_page_number = int(last_saved_count / 25)
    if last_saved_page_number == 0:
        last_saved_page_number = 1  # start with page 1 for the first time
    next_page_url = f'https://lda.senate.gov/api/v1/filings/?page_size=25&page={last_saved_page_number}'  # repeat in case the page has new items
    headers = {
        "Authorization": "Token " + federal_senate_lobbying_api_key
    }
    while time.time() - start_time < 520:
        next_page_url = loop(headers, next_page_url)
        if next_page_url is None:
            logger.info('FINISHED WITH LAST PAGE')
            break
        time.sleep(0.2)
    return True
