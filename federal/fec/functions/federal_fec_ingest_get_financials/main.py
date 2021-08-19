import logging
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, helpers
import requests
import datetime
import pytz
import json
import time

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
elastic_host = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_host/versions/1"}).payload.data.decode()
elastic_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_username_data/versions/1"}).payload.data.decode()
elastic_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_password_data/versions/1"}).payload.data.decode()
federal_fec_api_key = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/federal_fec_api_key/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)

# connect to fec api
def get(type, datestring, page):
    response = requests.get('https://api.open.fec.gov/v1/reports/' + type + '/?per_page=100&api_key=' + federal_fec_api_key + '&min_receipt_date=' + datestring + '&max_receipt_date=' + datestring + '&page=' + str(page))
    if response.status_code == 200:
        return json.loads(response.text)
    return False

# gets financials from FEC API and loads them into Elasticsearch
def federal_fec_ingest_get_financials(message, context):

    # set default datestring
    today = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))
    weeks_back = today.hour%4

    # set starting datestring
    datestring = (today-datetime.timedelta(weeks=weeks_back)).strftime("%Y-%m-%d")
    if 'attributes' in message:
        if message['attributes'] is not None:
            if "date" in message["attributes"]:
                datestring = message["attributes"]["date"]

    # configure ending datestring
    endstring = (datetime.datetime.strptime(datestring, '%Y-%m-%d')-datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    if 'attributes' in message:
        if message['attributes'] is not None:
            if "enddate" in message["attributes"]:
                endstring = message["attributes"]["enddate"]

    while datestring != endstring:

        # get new data for date
        actions = []
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
                # add results to actions
                for result in response["results"]:
                    actions.append({
                        "_op_type": "index",
                        "_index": "federal_fec_financials",
                        "_id": result["beginning_image_number"],
                        "_source": {
                            "obj": result,
                            "context": {
                                "last_indexed": datetime.datetime.now(datetime.timezone.utc)
                            }
                        }
                    })
                # iterate through pages
                page += 1
        # send results to Elasticsearch
        helpers.bulk(es, actions)
        actions = []
        # get next date
        datestring = (datetime.datetime.strptime(datestring, '%Y-%m-%d')-datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    return True
