import logging
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
from neo4j import GraphDatabase
import time
import datetime
import pytz
import hashlib
from simhash import Simhash

import cypher

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
neo4j_connection = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_connection/versions/1"}).payload.data.decode()
neo4j_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_username_data/versions/1"}).payload.data.decode()
neo4j_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_password_data/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
driver = GraphDatabase.driver(neo4j_connection, auth=(neo4j_username_data, neo4j_password_data))

# takes Facebook ads from ElasticSearch and load into Neo4j
def facebook_compute_load_ads(message, context):

    # configure ElasticSearch search
    s = Search(using=es, index="facebook_ads")
    q = s.exclude("term", in_graph="true")

    # get start time
    start = time.time()

    # loop for 520s
    while time.time()-start < 520:

        docs = q[0:100].sort("-obj.ad_creation_time").exclude("exists", field="meta.last_graphed").execute()
        if len(docs) == 0:
            logger.info(' - '.join(['NO ADS FOUND FOR LOADING']))
            break

        # prepare docs for loading
        actions = []
        ads_with_delivery_stop_time = []
        ads_without_delivery_stop_time = []
        creation_days = []
        delivery_days = []
        messages = []
        pages = []
        buyers = []
        states = []
        for doc in docs:
            # make ad objects
            try:
                creation_time = datetime.datetime.strptime(doc.obj.ad_creation_time, "%Y-%m-%dT%H:%M:%S%z")
            except:
                creation_time = datetime.datetime.strptime(doc.obj.ad_creation_time, "%Y-%m-%d")
            try:
                delivery_start_time = datetime.datetime.strptime(doc.obj.ad_delivery_start_time, "%Y-%m-%dT%H:%M:%S%z")
            except:
                delivery_start_time = datetime.datetime.strptime(doc.obj.ad_delivery_start_time, "%Y-%m-%d")
            ad = {
                "id": doc.obj.id,
                "creation_time": {
                    "year": creation_time.year,
                    "month": creation_time.month,
                    "day": creation_time.day,
                    "hour": creation_time.hour,
                    "minute": creation_time.minute
                },
                "delivery_start_time": {
                    "year": delivery_start_time.year,
                    "month": delivery_start_time.month,
                    "day": delivery_start_time.day,
                    "hour": delivery_start_time.hour,
                    "minute": delivery_start_time.minute
                },
                "impressions_lower_bound": None,
                "impressions_upper_bound": None,
                "spend_lower_bound": None,
                "spend_upper_bound": None,
                "potential_reach_lower_bound": None,
                "potential_reach_upper_bound": None,
                "creative_link_caption": None
            }
            if "impressions" in doc.obj:
                if "lower_bound" in doc.obj["impressions"]:
                    ad["impressions_lower_bound"] = doc.obj["impressions"]["lower_bound"]
                if "upper_bound" in doc.obj["impressions"]:
                    ad["impressions_upper_bound"] = doc.obj["impressions"]["upper_bound"]
            if "currency" in doc.obj:
                if doc.obj["currency"] == "USD" and "spend" in doc.obj:
                    if "lower_bound" in doc.obj["spend"]:
                        ad["spend_lower_bound"] = doc.obj["spend"]["lower_bound"]
                    if "upper_bound" in doc.obj["spend"]:
                        ad["spend_upper_bound"] = doc.obj["spend"]["upper_bound"]
            if "potential_reach" in doc.obj:
                if "lower_bound" in doc.obj["potential_reach"]:
                    ad["potential_reach_lower_bound"] = doc.obj["potential_reach"]["lower_bound"]
                if "upper_bound" in doc.obj["potential_reach"]:
                    ad["potential_reach_upper_bound"] = doc.obj["potential_reach"]["upper_bound"]
            if "ad_creative_link_caption" in doc.obj:
                ad["creative_link_caption"] = doc.obj["ad_creative_link_caption"]
            if "ad_delivery_stop_time" in doc.obj:
                try:
                    delivery_stop_time = datetime.datetime.strptime(doc.obj["ad_delivery_stop_time"], "%Y-%m-%dT%H:%M:%S%z")
                except:
                    delivery_stop_time = datetime.datetime.strptime(doc.obj["ad_delivery_stop_time"], "%Y-%m-%d")
                ad["delivery_stop_time"] = {
                    "year": delivery_stop_time.year,
                    "month": delivery_stop_time.month,
                    "day": delivery_stop_time.day,
                    "hour": delivery_stop_time.hour,
                    "minute": delivery_stop_time.minute
                }
                ads_with_delivery_stop_time.append(ad)
            else:
                ads_without_delivery_stop_time.append(ad)
            # make day objects
            creation_day = creation_time.astimezone(pytz.timezone('US/Eastern'))
            creation_days.append({
                "id": doc.obj.id,
                "year": creation_day.year,
                "month": creation_day.month,
                "day": creation_day.day,
            })
            delivery_start_day = delivery_start_time.astimezone(pytz.timezone('US/Eastern'))
            if "delivery_stop_time" in ad:
                delivery_stop_day = delivery_stop_time.astimezone(pytz.timezone('US/Eastern'))
                while delivery_start_day <= delivery_stop_day:
                    delivery_days.append({
                        "id": doc.obj.id,
                        "year": delivery_start_day.year,
                        "month": delivery_start_day.month,
                        "day": delivery_start_day.day
                    })
                    delivery_start_day += datetime.timedelta(days=1)
            # make message objects
            if "ad_creative_body" in doc.obj:
                messages.append({
                    "id": doc.obj.id,
                    "sha512": hashlib.sha512(doc.obj["ad_creative_body"].encode('utf-8')).hexdigest(),
                    "simhash": str(Simhash(doc.obj["ad_creative_body"]).value)
                })
            # make page objects
            if "page_id" in doc.obj:
                if "page_name" in doc.obj:
                    pages.append({
                        "id": doc.obj.id,
                        "page_name": doc.obj.page_name.upper().strip(),
                        "page_id": doc.obj.page_id,
                    })
                else:
                    pages.append({
                        "id": doc.obj.id,
                        "page_id": doc.obj.page_id,
                    })
            # make buyer objects
            if "funding_entity" in doc.obj:
                buyers.append({
                    "id": doc.obj.id,
                    "name": doc.obj["funding_entity"].upper().strip()
                })
            # make state objects
            if "regions" in doc.processed:
                for region in doc.processed.regions:
                    states.append({
                        "id": doc.obj.id,
                        "state": region.upper()
                    })
            # prepare to mark as in graph in elasticsearch
            actions.append({
                "_op_type": "update",
                "_index": "facebook_ads",
                "_id": doc.obj.id,
                "_source": {
                    "meta": {
                        "last_graphed": datetime.datetime.now(datetime.timezone.utc)
                    }
                }
            })
        # batch write to neo4j
        with driver.session() as neo4j:
            neo4j.write_transaction(cypher.merge_ads_with_delivery_stop_time, batch=ads_with_delivery_stop_time)
            neo4j.write_transaction(cypher.merge_ads_without_delivery_stop_time, batch=ads_without_delivery_stop_time)
            neo4j.write_transaction(cypher.merge_creation_days, batch=creation_days)
            neo4j.write_transaction(cypher.merge_delivery_days, batch=delivery_days)
            neo4j.write_transaction(cypher.merge_messages, batch=messages)
            neo4j.write_transaction(cypher.merge_pages, batch=pages)
            neo4j.write_transaction(cypher.merge_buyers, batch=buyers)
            neo4j.write_transaction(cypher.merge_states, batch=states)
        # batch write to elasticsearch
        helpers.bulk(es, actions)
        logger.info(' - '.join(['ADS PROCESSED', str(len(actions))]))

    # return true
    return True
