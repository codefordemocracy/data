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

# get timeline tweets from user id and update Firestore and ElasticSearch
def twitter_ingest_get_timeline(message, context):

    # get id from the Pub/Sub message
    id = int(message['attributes']['id'])

    # set up Firestore refs
    ref = db.collection('twitter').document('queues').collection('tweet')

    # grab latest cursor data
    userdoc = es.get(index="twitter_users_new", id=id, _source_includes=["cursors"])
    max_id = -1
    min_id = -1
    direction = None
    end = False
    if "cursors" in userdoc["_source"]:
        cursors = userdoc["_source"]["cursors"]
        max_id = int(cursors["max_id"])
        min_id = int(cursors["min_id"])
        if "direction" in cursors:
            direction = cursors["direction"]
        if "end" in cursors:
            end = cursors["end"]

    # format API settings
    settings = {
        "expansions": ["author_id", "referenced_tweets.id", "in_reply_to_user_id", "attachments.media_keys", "attachments.poll_ids", "geo.place_id", "entities.mentions.username", "referenced_tweets.id.author_id"],
        "tweet.fields": ["id", "text", "attachments", "author_id", "context_annotations", "conversation_id", "created_at", "entities", "geo", "in_reply_to_user_id", "lang", "possibly_sensitive", "public_metrics", "referenced_tweets", "reply_settings", "source", "withheld"],
        "user.fields": ["id", "name", "username", "created_at", "description", "entities", "location", "pinned_tweet_id", "profile_image_url", "protected", "public_metrics", "url", "verified", "withheld"],
        "poll.fields": ["id", "options", "duration_minutes", "end_datetime", "voting_status"],
        "media.fields": ["media_key", "type", "duration_ms", "height", "preview_image_url", "public_metrics", "width", "alt_text"],
        "place.fields": ["full_name", "id", "contained_within", "country", "country_code", "geo", "name", "place_type"],
    }
    settings_string = "?max_results=100"
    for key in settings:
        settings_string += "&" + key + "=" + ",".join(settings[key])

    # set up API call
    url = "https://api.twitter.com/2/users/" + str(id) + "/tweets"
    headers = {
        "Authorization": "Bearer " + twitter_token
    }
    # if this is a new user, start with the first page
    if max_id == -1 and min_id == -1:
        logger.info(' - '.join(['DOWNLOADING TIMELINE', 'first', str(id)]))
        url += settings_string
    # traverse to get older tweets if they haven't yet been exhausted
    elif direction == "older":
        logger.info(' - '.join(['DOWNLOADING TIMELINE', 'until_id', str(id), str(min_id)]))
        url += settings_string + "&until_id=" + str(min_id)
    # otherwise, use the max_id to get newer tweets
    else:
        logger.info(' - '.join(['DOWNLOADING TIMELINE', 'since_id', str(id), str(max_id)]))
        url += settings_string + "&since_id=" + str(max_id)

    # get data from API
    r = requests.get(url, headers=headers)
    try:
        assert r.status_code == 200
        response = json.loads(r.text)
    except Exception as e:
        response = None
        logger.error(' - '.join(['COULD NOT DOWNLOAD TIMELINE', str(r.status_code), str(id), str(e)]))

    # parse tweets
    actions = []
    if response:

        data = response.get("data", [])
        includes = response.get("includes", {})

        if direction == "older" and len(data) == 1:
            end = True

        for tweet in data:

            # set min and max tweet ids
            if max_id == -1:
                max_id = int(tweet["id"])
            if min_id == -1:
                min_id = int(tweet["id"])
            max_id = max(max_id, int(tweet["id"]))
            min_id = min(min_id,int(tweet["id"]))

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

    if end is True:
        direction = "newer"
    else:
        if direction == "older":
            direction = "newer"
        else:
            direction = "older"

    # prep the user record update
    actions.append({
        "_op_type": "update",
        "_index": "twitter_users_new",
        "_id": str(id),
        "doc": {
            "cursors": {
                "min_id": min_id,
                "max_id": max_id,
                "direction": direction,
                "end": end
            },
            "context": {
                "last_updated": datetime.datetime.now(datetime.timezone.utc)
            }
        }
    })

    # bulk update elasticsearch
    helpers.bulk(es, actions)
    logger.info(' - '.join(['DOCS SYNCED TO ELASTICSEARCH', str(len(actions))]))

    return id
