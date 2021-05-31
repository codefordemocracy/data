import logging
from google.cloud import secretmanager
from google.cloud import firestore
from elasticsearch import Elasticsearch, helpers
import requests
import json
import datetime
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
facebook_client_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/facebook_client_id/versions/1"}).payload.data.decode()
facebook_client_secret = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/facebook_client_secret/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
db = firestore.Client()

# looping function for sending and processing requests
def loop(token, after, errors):

    # build the request to get ads
    url = "https://graph.facebook.com/v6.0/ads_archive/"
    url += "?fields=id,ad_creation_time,ad_creative_body,ad_creative_link_caption,ad_creative_link_description,ad_creative_link_title,ad_delivery_start_time,ad_delivery_stop_time,ad_snapshot_url,currency,demographic_distribution,funding_entity,impressions,page_id,page_name,potential_reach,publisher_platforms,region_distribution,spend"
    url += "&ad_active_status=ALL"
    url += "&ad_type=POLITICAL_AND_ISSUE_ADS"
    url += "&ad_reached_countries=['US']"
    url += "&search_terms=''"
    url += "&limit=1000"
    if after is not None:
        url += "&after=" + after
    headers = {
        "Authorization": "Bearer " + token
    }

    # send request and get basic info
    r = requests.get(url, headers=headers)
    ads = None
    num = 0
    try:
        ads = json.loads(r.text)
        num = len(ads["data"])
    except:
        logger.error(' - '.join(['NO ADS RETRIEVED', r.text]))
        errors += 1
        pass
    try:
        after = ads["paging"]["cursors"]["after"]
    except:
        logger.info(' - '.join(['NO MORE ADS', str(after), r.text]))
        errors += 1
        pass

    # iterate through ads and index to ElasticSearch
    if num > 0:

        # load most recent during week, keep going on weekends
        weekday = True
        if datetime.datetime.today().weekday() >= 5:
            weekday = False

        load = False
        if weekday:
            # check if both first and last ad is already in ElasticSearch
            try:
                first = es.get(index="facebook_ads", id=ads["data"][0]["id"], _source=False)
                last = es.get(index="facebook_ads", id=ads["data"][-1]["id"], _source=False)
                logger.info(' - '.join(['NO NEED TO LOAD ADS', str(after)]))
                after = None
            # load if the ads are new
            except:
                load = True
                pass
        else:
            load = True

        # logic for loading
        if load:
            actions = []
            for ad in ads["data"]:
                processed = dict()
                if "region_distribution" in ad:
                    processed["regions"] = []
                    for region in ad["region_distribution"]:
                        processed["regions"].append(region["region"])
                actions.append({
                    "_op_type": "index",
                    "_index": "facebook_ads",
                    "_id": ad["id"],
                    "_source": {
                        "obj": ad,
                        "processed": processed,
                        "meta": {
                            "last_indexed": datetime.datetime.now(datetime.timezone.utc)
                        }
                    }
                })
            helpers.bulk(es, actions)
            logger.info(' - '.join(['ADS INDEXED', str(num)]))

    return {"after": after, "errors": errors}

# indexes Facebook ads into ElasticSearch
def facebook_ingest_get_ads(message, context):

    ref = db.collection('facebook').document('ads')

    # grab settings
    settings = ref.get().to_dict()
    after = settings["after"]
    token = settings["token"]

    # get new token once per day
    if settings["last_updated"].date() != datetime.datetime.utcnow().date():
        url = "https://graph.facebook.com/v6.0/oauth/access_token?grant_type=fb_exchange_token&client_id=" + facebook_client_id + "&client_secret=" + facebook_client_secret + "&fb_exchange_token="
        url += token
        r = requests.get(url)
        try:
            token = json.loads(r.text)["access_token"]
        except:
            logger.error(' - '.join(['NEW TOKEN COULD NOT BE OBTAINED']))
            pass

    # get start time
    start = time.time()

    # counter for errors
    errors = 0

    # loop for 520s
    while time.time()-start < 520:
        # get ads
        result = loop(token, after, errors)
        after = result["after"]
        errors = result["errors"]
        # reset counter if lots of errors
        if errors > 5:
            after = None
            break
        # either stop or wait to loop
        if time.time()-start >= 475:
            break
        else:
            # wait for 60 seconds
            # rate limit is stated to be 200 requests per hour but actually ~1600 per day
            time.sleep(60)

    # update Firestore
    settings["token"] = token
    settings["after"] = after
    settings["last_updated"] = datetime.datetime.now(datetime.timezone.utc)
    ref.set(settings)
    logger.info(' - '.join(['FIRESTORE UPDATED', str(after)]))

    # return true
    return True
