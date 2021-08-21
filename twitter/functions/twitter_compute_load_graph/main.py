import logging
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search, Q
from google.cloud import secretmanager
from google.cloud import firestore
from neo4j import GraphDatabase
import json
import datetime
import pytz

import cypher
import utilities

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
db = firestore.Client()

# helper function to parse tweet
def parse_tweet(obj):
    parsed = {
        "tweets": [],
        "tweeters": [],
        "hashtags": [],
        "mentions": [],
        "annotations": [],
        "links": []
    }
    # make tweet objects
    dt = datetime.datetime.strptime(obj['tweet']['created_at'], "%Y-%m-%dT%H:%M:%S.%f%z")
    est = dt.astimezone(pytz.timezone("America/New_York"))
    text = obj['tweet']['text'].splitlines()
    text = list(filter(None, text))
    parsed["tweets"].append({
        "tweet_id": obj['tweet']['id'],
        "year": dt.year,
        "month": dt.month,
        "day": dt.day,
        "hour": dt.hour,
        "minute": dt.minute,
        "est_year": est.year,
        "est_month": est.month,
        "est_day": est.day,
        "summary": " ".join(text),
        "url": "twitter.com/"+obj['author']['username']+"/status/"+obj['tweet']['id'],
        "user_id": obj['author']['id'],
        "username": obj['author']['username'].upper()
    })
    parsed["tweeters"].append({
        "user_id": obj['author']['id'],
        "username": obj['author']['username'].upper(),
        "name": obj['author']['name'],
        "verified": obj['author']['verified']
    })
    if obj['tweet'].get("entities") is not None:
        # make hashtag objects
        if "hashtags" in obj['tweet']['entities']:
            for tag in obj['tweet']['entities']['hashtags']:
                parsed["hashtags"].append({
                    "tweet_id": obj['tweet']['id'],
                    "hashtag": tag['tag'].upper()
                })
        # make mentions objects
        if "mentions" in obj['tweet']['entities']:
            for mention in obj['tweet']['entities']['mentions']:
                parsed["mentions"].append({
                    "tweet_id": obj['tweet']['id'],
                    "user_id": mention['id'],
                    "username": mention['username'].upper(),
                })
        # make annotation objects
        if "annotations" in obj['tweet']['entities']:
            for annotation in obj['tweet']['entities']['annotations']:
                parsed["annotations"].append({
                    "tweet_id": obj['tweet']['id'],
                    "text": annotation['normalized_text'].upper(),
                    "type": annotation['type'],
                    "probability": annotation['probability']
                })
        # make link objects
        if "urls" in obj['tweet']['entities']:
            for link in obj['tweet']['entities']['urls']:
                if 'twitter.com' not in link['expanded_url']:
                    clean_url = utilities.strip_url(link['expanded_url'])
                    parsed["links"].append({
                        "tweet_id": obj['tweet']['id'],
                        "url": clean_url
                    })
    # return parsed tweet
    return parsed

# loads tweets from Elasticsearch into Neo4j
def twitter_compute_load_graph(message, context):

    user_id = None
    tweet_ids = None

    # set up Firestore ref for queue of tweets to load
    ref = db.collection('twitter').document('queues').collection('graph')

    # get the optional search term, user_id, or tweet_ids from the Pub/Sub message
    if 'attributes' in message:
        if message['attributes'] is not None:
            if 'user_id' in message['attributes']:
                user_id = message['attributes']['user_id']
                logger.info(' - '.join(['INFO', 'search tweets by user_id', user_id]))
            if 'tweet_ids' in message['attributes']:
                tweet_ids = json.loads(message['attributes']['tweet_ids'])
                logger.info(' - '.join(['INFO', 'search tweets by tweet_ids', str(tweet_ids)]))

    # search ElasticSearch
    docs = []
    s = Search(using=es, index="twitter_tweets_new").exclude("exists", field="last_graphed")
    if user_id is not None:
        q = s.filter("term", obj__author__id=user_id).query(Q('bool', should=[Q("exists", field='obj.tweet.referenced_tweets.id'), Q("exists", field='obj.tweet.entities.urls.expanded_url')], minimum_should_match=1))
        docs = q[0:30].sort("-obj.tweet.created_at").execute()
    elif tweet_ids is not None:
        q = s.query("ids", values=tweet_ids)
        docs = q[0:10000].execute()

    # set up array for tweet_ids that will need to be processed
    ids = []

    # process tweets with text
    actions = []
    tweets = []
    tweeters = []
    hashtags = []
    mentions = []
    annotations = []
    links = []
    quotes = []
    replies = []
    retweets = []
    for doc in docs:
        document = doc.to_dict()
        # parse the main tweet
        parsed = parse_tweet(document["obj"])
        tweets.extend(parsed["tweets"])
        tweeters.extend(parsed["tweeters"])
        hashtags.extend(parsed["hashtags"])
        mentions.extend(parsed["mentions"])
        annotations.extend(parsed["annotations"])
        links.extend(parsed["links"])
        # make quote objects
        if "quoted" in document['obj']:
            if "tweet" in document['obj']["quoted"]:
                quotes.append({
                    "tweet_id": document['obj']['tweet']['id'],
                    "quote_tweet_id": document['obj']['quoted']['tweet']['id']
                })
                parsed_quote = parse_tweet(document["obj"]["quoted"])
                tweets.extend(parsed_quote["tweets"])
                tweeters.extend(parsed_quote["tweeters"])
                hashtags.extend(parsed_quote["hashtags"])
                mentions.extend(parsed_quote["mentions"])
                annotations.extend(parsed_quote["annotations"])
                links.extend(parsed_quote["links"])
                ids.append(document['obj']['quoted']['tweet']['id'])
        # make reply objects
        if "replied_to" in document['obj']:
            if "tweet" in document['obj']["replied_to"]:
                replies.append({
                    "tweet_id": document['obj']['tweet']['id'],
                    "reply_tweet_id": document['obj']['replied_to']['tweet']['id']
                })
                parsed_reply = parse_tweet(document["obj"]["replied_to"])
                tweets.extend(parsed_reply["tweets"])
                tweeters.extend(parsed_reply["tweeters"])
                hashtags.extend(parsed_reply["hashtags"])
                mentions.extend(parsed_reply["mentions"])
                annotations.extend(parsed_reply["annotations"])
                links.extend(parsed_reply["links"])
                ids.append(document['obj']['replied_to']['tweet']['id'])
        # make retweeted objects
        if "retweeted" in document['obj']:
            if "tweet" in document['obj']["retweeted"]:
                retweets.append({
                    "tweet_id": document['obj']['tweet']['id'],
                    "retweet_id": document['obj']['retweeted']['tweet']['id'],
                })
                parsed_retweet = parse_tweet(document["obj"]["retweeted"])
                tweets.extend(parsed_retweet["tweets"])
                tweeters.extend(parsed_retweet["tweeters"])
                hashtags.extend(parsed_retweet["hashtags"])
                mentions.extend(parsed_retweet["mentions"])
                annotations.extend(parsed_retweet["annotations"])
                links.extend(parsed_retweet["links"])
                ids.append(document['obj']['retweeted']['tweet']['id'])
        # prepare to mark as in graph in elasticsearch
        actions.append({
            "_op_type": "update",
            "_index": "twitter_tweets_new",
            "_id": doc.meta.id,
            "doc": {
                "context": {
                    "last_graphed": datetime.datetime.now(datetime.timezone.utc)
                }
            },
            "retry_on_conflict": 3
        })

    # batch write to neo4j
    with driver.session() as neo4j:
        neo4j.write_transaction(cypher.merge_tweets, batch=tweets)
        neo4j.write_transaction(cypher.merge_tweeters, batch=tweeters)
        neo4j.write_transaction(cypher.merge_hashtags, batch=hashtags)
        neo4j.write_transaction(cypher.merge_mentions, batch=mentions)
        neo4j.write_transaction(cypher.merge_annotations, batch=annotations)
        neo4j.write_transaction(cypher.merge_links, batch=links)
        neo4j.write_transaction(cypher.merge_quotes, batch=quotes)
        neo4j.write_transaction(cypher.merge_replies, batch=replies)
        neo4j.write_transaction(cypher.merge_retweets, batch=retweets)

    # add tweet_ids of tweets that need to be processed to queue
    ids = list(set(ids))
    for id in ids:
        ref.document(id).set({"last_added": datetime.datetime.now(datetime.timezone.utc)})

    # if using a user_id, mark the user as having been graphed
    if user_id is not None:
        actions.append({
            "_op_type": "update",
            "_index": "twitter_users_new",
            "_id": user_id,
            "doc": {
                "context": {
                    "last_graphed": datetime.datetime.now(datetime.timezone.utc)
                }
            },
            "retry_on_conflict": 3
        })

    # batch write to elasticsearch
    helpers.bulk(es, actions)

    # remove processed tweets from firestore graph queue
    if user_id is not None:
        for action in actions:
            if action["_index"] == "twitter_tweets_new":
                # remove processed tweets from queue
                ref.document(action["_id"]).delete()
    if tweet_ids is not None:
        for i in tweet_ids:
            ref.document(i).delete()

    logger.info(' - '.join(['TWEETS PROCESSED', str(len([a for a in actions if a["_index"] == "twitter_tweets_new"])), str(len(ids))]))
    return len(actions)
