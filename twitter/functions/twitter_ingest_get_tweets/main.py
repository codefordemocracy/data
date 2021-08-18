import logging
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import pubsub
from elasticsearch import Elasticsearch, helpers
import requests
import datetime
import json

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
twitter_token = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/twitter_token/versions/1"}).payload.data.decode()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
db = firestore.Client()
publisher = pubsub.PublisherClient()

# get a list of tweets and update Firestore and ElasticSearch
def twitter_ingest_get_tweets(message, context):

    # get the chunk of ids from the Pub/Sub message
    chunk1 = json.loads(message['attributes']['chunk1'])
    chunk2 = json.loads(message['attributes']['chunk2'])
    chunk3 = json.loads(message['attributes']['chunk3'])
    chunk = chunk1 + chunk2 + chunk3
    chunk_string = json.dumps(chunk)
    chunk_string = chunk_string[1:-1].replace(" ", "")

    # set up Firestore refs
    ref = db.collection('twitter').document('queues').collection('tweet')

    # format API settings
    settings = {
        "expansions": ["author_id", "referenced_tweets.id", "in_reply_to_user_id", "attachments.media_keys", "attachments.poll_ids", "geo.place_id", "entities.mentions.username", "referenced_tweets.id.author_id"],
        "tweet.fields": ["id", "text", "attachments", "author_id", "context_annotations", "conversation_id", "created_at", "entities", "geo", "in_reply_to_user_id", "lang", "possibly_sensitive", "public_metrics", "referenced_tweets", "reply_settings", "source", "withheld"],
        "user.fields": ["id", "name", "username", "created_at", "description", "entities", "location", "pinned_tweet_id", "profile_image_url", "protected", "public_metrics", "url", "verified", "withheld"],
        "poll.fields": ["id", "options", "duration_minutes", "end_datetime", "voting_status"],
        "media.fields": ["media_key", "type", "duration_ms", "height", "preview_image_url", "public_metrics", "width", "alt_text"],
        "place.fields": ["full_name", "id", "contained_within", "country", "country_code", "geo", "name", "place_type"],
    }
    settings_string = "?ids=" + chunk_string
    for key in settings:
        settings_string += "&" + key + "=" + ",".join(settings[key])

    # set up API call
    url = "https://api.twitter.com/2/tweets" + settings_string
    headers = {
        "Authorization": "Bearer " + twitter_token
    }

    # get data from API
    r = requests.get(url, headers=headers)
    try:
        assert r.status_code == 200
        response = json.loads(r.text)
    except Exception as e:
        response = None
        logger.error(' - '.join(['COULD NOT DOWNLOAD TWEETS', str(r.status_code), str(e)]))

    # parse tweets
    actions = []
    if response:

        data = response.get("data", [])
        includes = response.get("includes", {})

        for tweet in data:

            # construct the document
            doc = {"tweet": tweet}

            # hydrate all the missing parts
            if "author_id" in tweet:
                doc["author"] = [u for u in includes["users"] if u["id"] == tweet["author_id"]][0]
            if "referenced_tweets" in tweet:
                for tw in tweet["referenced_tweets"]:
                    doc[tw["type"]] = dict()
                    try:
                        doc[tw["type"]]["tweet"] = [t for t in includes["tweets"] if t["id"] == tw["id"]][0]
                    except:
                        logger.info(' - '.join(['INFO', tw["type"], 'missing included tweet', tw["id"]]))
                        pass
                    try:
                        if "author_id" in doc[tw["type"]]["tweet"]:
                            doc[tw["type"]]["author"] = [u for u in includes["users"] if u["id"] == doc[tw["type"]]["tweet"]["author_id"]][0]
                    except:
                        logger.info(' - '.join(['INFO', tw["type"], 'missing included user for tweet', tw["id"]]))
                        pass
                    # also add to the tweets Firestore queue
                    ref.document(tw["id"]).set({"last_added": datetime.datetime.now(datetime.timezone.utc)})

            # process article links
            if "entities" in doc['tweet']:
                if "urls" in doc['tweet']['entities']:
                    for url in doc['tweet']['entities']['urls']:
                        if 'twitter.com' not in url['expanded_url']:
                            # send article to be scraped
                            topic = 'projects/' + gcp_project_id + '/topics/news_articles_ingest_get_url'
                            publisher.publish(topic, b'scrape this url', url=url['expanded_url'])
                            logger.info(' - '.join(['INFO', 'url sent to news_articles_ingest_get_url queue', url['expanded_url']]))

            # prep tweet doc for elasticsearch
            actions.append({
                "_op_type": "index",
                "_index": "twitter_tweets_new",
                "_id": doc["tweet"]["id"],
                "_source": {
                    "obj": doc,
                    "context": {
                        "last_indexed": datetime.datetime.now(datetime.timezone.utc),
                        "last_updated": datetime.datetime.now(datetime.timezone.utc),
                        "api_version": 2
                    }
                }
            })

        for user in includes.get("users", []):

            # prep user for elasticsearch
            actions.append({
                "_op_type": "update",
                "_index": "twitter_users_new",
                "_id": user["id"],
                "doc": {
                    "obj": user,
                    "context": {
                        "last_indexed": datetime.datetime.now(datetime.timezone.utc),
                        "last_updated": datetime.datetime.now(datetime.timezone.utc),
                        "api_version": 2
                    }
                },
                "doc_as_upsert": True,
                "retry_on_conflict": 3
            })

        # bulk update elasticsearch
        helpers.bulk(es, actions)
        logger.info(' - '.join(['DOCS SYNCED TO ELASTICSEARCH', str(len(actions))]))

        if len(data) > 0:
            for i in chunk:
                # delete from Firestore
                ref.document(str(i)).delete()
            logger.info(' - '.join(['COMPLETED', 'tweets deleted from Firestore', str(len(chunk))]))

    return id
