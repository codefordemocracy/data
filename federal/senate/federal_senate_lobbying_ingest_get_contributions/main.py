import time
import datetime
import logging

import requests
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, helpers
import pytz
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
index = 'federal_senate_lobbying_contributions'

# helper loop
def loop(headers, url):
    actions = []
    r = requests.get(url, headers=headers)
    for filing in r.json()['results']:
        key = filing.get('filing_uuid')
        processed = {
            "date_submitted": filing.get("dt_posted"),
            "filing_year": filing.get("filing_year"),
            "registrant": {
                "name": filing.get("registrant", {}).get("name"),
                "description": filing.get("registrant", {}).get("description"),
                "country": filing.get("registrant", {}).get("country"),
                "state": filing.get("registrant", {}).get("state"),
                "senate_id": str(filing.get("registrant", {}).get("id")),
                "house_id": str(filing.get("registrant", {}).get("house_registrant_id")),
                "contact": filing.get("registrant", {}).get("contact_name"),
            },
            "no_contributions": filing.get("no_contributions"),
            "url": filing.get("filing_document_url")
        }
        if filing.get("pacs") is not None:
            if len(filing.get("pacs")) > 0:
                processed["pacs"] = filing.get("pacs")
        lobbyist = {}
        if filing.get("lobbyist") is not None:
            if filing.get("lobbyist", {}).get("id") is not None:
                lobbyist["id"] = filing.get("lobbyist", {}).get("id")
            name = [filing.get("lobbyist", {}).get("first_name"), filing.get("lobbyist", {}).get("middle_name"), filing.get("lobbyist", {}).get("last_name"), filing.get("lobbyist", {}).get("suffix")]
            name = [n for n in name if n is not None]
            if len(name) > 0:
                lobbyist["name"] = " ".join(name)
            if len(lobbyist) > 0:
                processed["lobbyist"] = lobbyist
        contributions = []
        if filing.get("contribution_items") is not None:
            for c in filing.get("contribution_items"):
                dt = c.get("date")
                if dt is not None:
                    dt = datetime.datetime.strptime(dt, '%Y-%m-%d')
                    dt = pytz.timezone('US/Eastern').localize(dt)
                    dt = dt.strftime("%Y-%m-%dT%H:%M:%S%z")
                contributions.append({
                    "contribution_type": c.get("contribution_type_display"),
                    "contributor_name": c.get("contributor_name"),
                    "payee_name": c.get("payee_name"),
                    "recipient_name": c.get("honoree_name"),
                    "amount": float(c.get("amount").replace(',', '')) if c.get("amount") is not None else None,
                    "date": dt
                })
        if len(contributions) > 0:
            processed["contributions"] = contributions
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

# indexes Senate lobbying contributions into ElasticSearch
def federal_senate_lobbying_ingest_get_contributions(message, context):
    es.indices.refresh()
    last_saved_count = int(es.cat.count(index, params={"format": "json"})[0]['count'])
    next_page = None
    start_time = time.time()
    last_saved_page_number = int(last_saved_count / 25)
    if last_saved_page_number == 0:
        last_saved_page_number = 1  # start with page 1 for the first time
    next_page_url = f'https://lda.senate.gov/api/v1/contributions/?page_size=25&page={last_saved_page_number}'  # repeat in case the page has new items
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
