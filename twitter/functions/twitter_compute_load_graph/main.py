import logging
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import pubsub
from azure.cosmosdb.table.tableservice import TableService
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
azure_account_name_twitter = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/azure_account_name_twitter/versions/1"}).payload.data.decode()
azure_account_key_twitter = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/azure_account_key_twitter/versions/1"}).payload.data.decode()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
driver = GraphDatabase.driver(neo4j_connection, auth=(neo4j_username_data, neo4j_password_data))
table_service = TableService(account_name=azure_account_name_twitter, account_key=azure_account_key_twitter)
db = firestore.Client()
publisher = pubsub.PublisherClient()

# utility function to divide rows into batches of n
def chunks(l, n):
    return [l[i:i + n] for i in range(0, len(l), n)]

# loads tweets from Elasticsearch into Neo4j
def twitter_compute_load_graph(message, context):

    term = None
    user_id = None
    tweet_ids = None

    # set up Firestore refs
    quote_ref = db.collection('twitter').document('relationships').collection('quotes')
    reply_ref = db.collection('twitter').document('relationships').collection('replies')
    retweet_ref = db.collection('twitter').document('relationships').collection('retweets')

    # get the optional search term, user_id, or user_screen_name from the Pub/Sub message
    if 'attributes' in message:
        if message['attributes'] is not None:
            if 'term' in message['attributes']:
                term = message['attributes']['term']
                logger.info(' - '.join(['INFO', 'search tweet text for term', term]))
            if 'user_id' in message['attributes']:
                user_id = message['attributes']['user_id']
                logger.info(' - '.join(['INFO', 'search tweets by user_id', user_id]))
            if 'tweet_ids' in message['attributes']:
                tweet_ids = json.loads(message['attributes']['tweet_ids'])
                logger.info(' - '.join(['INFO', 'search tweets by tweet_ids', str(tweet_ids)]))

    # search ElasticSearch
    docs = []
    s = Search(using=es, index="tweets")
    if term is not None:
        if len(term.split()) > 1:
            query_type = "match_phrase"
        else:
            query_type = "match"
        q = s.filter("term", in_graph="false").query(query_type, obj__text=term)
        docs = q[0:30].sort("-obj.created_at").execute()
    elif user_id is not None:
        q = s.filter("term", in_graph="false").query("term", user_id=user_id)
        docs = q[0:30].sort("-obj.created_at").execute()
    elif tweet_ids is not None:
        q = s.filter("term", in_graph="false").query("ids", values=tweet_ids)
        docs = q.execute()

    # grab 30 newest retweets for loading
    rets = []
    if user_id is not None:
        rets = retweet_ref.where('source','==', user_id).where('in_graph', '==', False).order_by('created_at', direction=firestore.Query.DESCENDING).limit(30).stream()

    # set up array for tweet_ids that will need to be processed
    ids = []

    # process tweets with text
    actions = []
    tweets = []
    hashtags = []
    mentions = []
    links = []
    quotes = []
    replies = []
    users = []
    for doc in docs:
        document = doc.to_dict()
        # make tweet objects
        dt = datetime.datetime.strptime(document['obj']['created_at']+"+0000", "%Y-%m-%dT%H:%M:%S%z")
        est = dt.astimezone(pytz.timezone("America/New_York"))
        text = document['obj']['text'].splitlines()
        text = list(filter(None, text))
        tweets.append({
            "tweet_id": doc.id,
            "year": dt.year,
            "month": dt.month,
            "day": dt.day,
            "hour": dt.hour,
            "minute": dt.minute,
            "est_year": est.year,
            "est_month": est.month,
            "est_day": est.day,
            "summary": " ".join(text),
            "url": "twitter.com/"+document['user_screen_name']+"/status/"+doc.id,
            "user_id": document['user_id'],
            "screen_name": document['user_screen_name'].lower()
        })
        users.append(document['user_id'])
        # make hashtag objects
        for tag in document['obj']['entities']['hashtags']:
            hashtags.append({
                "tweet_id": doc.id,
                "hashtag": tag['text'].upper()
            })
        # make mentions objects
        for mention in document['obj']['entities']['user_mentions']:
            mentions.append({
                "tweet_id": doc.id,
                "user_id": mention['id_str'],
                "screen_name": mention['screen_name'].lower(),
                "name": mention['name']
            })
        # make link objects
        for link in document['obj']['entities']['urls']:
            if 'twitter.com' not in link['expanded_url']:
                clean_url = utilities.strip_url(link['expanded_url'])
                links.append({
                    "tweet_id": doc.id,
                    "url": clean_url
                })
        # make quote objects
        for quote in quote_ref.where('source','==', doc['id']).stream():
            quote = quote.to_dict()
            dt = quote['created_at']
            quotes.append({
                "tweet_id": doc.id,
                "quote_tweet_id": quote['target']
            })
            ids.append(quote['target'])
        # make reply objects
        for reply in reply_ref.where('source','==', doc['id']).stream():
            reply = reply.to_dict()
            dt = reply['created_at']
            replies.append({
                "tweet_id": doc.id,
                "reply_tweet_id": reply['target']
            })
            ids.append(reply['target'])
        # prepare to mark as in graph in elasticsearch
        actions.append({
            "_op_type": "update",
            "_index": "tweets",
            "_id": doc.id,
            "script": {
                "source": "ctx._source.in_graph = true",
                "lang": "painless"
            }
        })
    # hydrate tweeters
    tweeters = []
    users = list(set(users))
    for user in users:
        for row in table_service.query_entities('users', filter="PartitionKey eq '" + user + "' and RowKey eq '" + user + "'"):
            tweeters.append({
                "user_id": user,
                "screen_name": row.screen_name.lower(),
                "name": row.name,
                "verified": row.verified,
                "description": row.description
            })

    # process retweets
    retweets = []
    for retweet in rets:
        retweet = retweet.to_dict()
        dt = retweet['created_at']
        est = dt.astimezone(pytz.timezone("America/New_York"))
        retweet_id = None
        if "retweet_id" in retweet:
            retweet_id = retweet['retweet_id']
        else:
            for row in table_service.query_entities('tweets', filter="PartitionKey eq '" + retweet["source"] + "' and is_retweet eq true and created_at eq datetime'" + retweet["created_at"].strftime("%Y-%m-%dT%H:%M:%S.000Z") + "'"):
                retweet_id = row.RowKey
        retweets.append({
            "user_id": retweet['source'],
            "tweet_id": retweet['target'],
            "retweet_id": retweet_id,
            "year": dt.year,
            "month": dt.month,
            "day": dt.day,
            "hour": dt.hour,
            "minute": dt.minute,
            "est_year": est.year,
            "est_month": est.month,
            "est_day": est.day
        })
        ids.append(retweet['target'])

    # batch write to neo4j
    with driver.session() as neo4j:
        neo4j.write_transaction(cypher.merge_tweets, batch=tweets)
        neo4j.write_transaction(cypher.merge_tweeters, batch=tweeters)
        neo4j.write_transaction(cypher.merge_hashtags, batch=hashtags)
        neo4j.write_transaction(cypher.merge_mentions, batch=mentions)
        neo4j.write_transaction(cypher.merge_links, batch=links)
        neo4j.write_transaction(cypher.merge_quotes, batch=quotes)
        neo4j.write_transaction(cypher.merge_replies, batch=replies)
        neo4j.write_transaction(cypher.merge_retweets, batch=retweets)

    # kick off graph load again with tweet_ids
    ids = list(set(ids))
    for chunk in chunks(ids, 30):
        topic = 'projects/' + gcp_project_id + '/topics/twitter_compute_load_graph'
        publisher.publish(topic, b'load tweets into graph', tweet_ids=json.dumps(chunk))
        logger.info(' - '.join(['INFO', 'ids sent to twitter_compute_load_graph queue', json.dumps(chunk)]))

    # batch write to elasticsearch
    helpers.bulk(es, actions)

    # update firestore for retweets
    for retweet in retweets:
        record = {"in_graph": True, "retweet_id": retweet["retweet_id"], "last_loaded": datetime.datetime.now(datetime.timezone.utc)}
        retweet_ref.document(retweet['user_id']+'_'+retweet['tweet_id']).set(record, merge=True)

    logger.info(' - '.join(['TWEETS PROCESSED', str(len(actions)), str(len(retweets))]))
    return len(actions)
