import logging
import praw
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, helpers
import datetime
import pytz
from bs4 import BeautifulSoup

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
reddit_client_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/reddit_client_id/versions/1"}).payload.data.decode()
reddit_client_secret = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/reddit_client_secret/versions/1"}).payload.data.decode()
reddit_user_agent = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/reddit_user_agent/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)

# connect to Reddit API
reddit = praw.Reddit(client_id=reddit_client_id, client_secret=reddit_client_secret, user_agent=reddit_user_agent)

# indexes Reddit ads into ElasticSearch
def reddit_ingest_get_ads(message, context):

    actions = []
    for submission in reddit.subreddit('RedditPoliticalAds').new(limit=None):
        obj = vars(submission)
        obj.pop("_reddit")
        obj["author"] = obj["author"].name
        obj["subreddit"] = obj["subreddit"].display_name
        obj["created"] = datetime.datetime.utcfromtimestamp(obj["created"])
        obj["created_utc"] = datetime.datetime.utcfromtimestamp(obj["created_utc"]).replace(tzinfo=pytz.UTC)
        if obj["edited"] is not False:
            obj["edited"] = True
        processed = dict()
        if obj["selftext_html"] is not None:
            soup = BeautifulSoup(obj["selftext_html"], 'html.parser')
            text = soup.get_text()
            text = text.replace("Ad Buyer Information and Related Data:", "")
            text = text.replace("Ad Duration:", "")
            text = text.replace("Subreddits:\n\n", "Subreddits:")
            text = text.replace("\nr/", ", r/")
            text = text.replace(":,", ":")
            for line in text.splitlines():
                if ": " in line:
                    key = line.split(": ")[0]
                    key = key.lower()
                    key = key.replace("-", "_")
                    key = key.replace("/", "_")
                    key = key.replace(" ", "_")
                    key = "".join(x for x in key if x.isalpha() or x is "_")
                    if key.endswith("_"):
                        key = key[:-1]
                    value = line.split(": ")[1]
                    try:
                        value = datetime.datetime.strptime(value, '%m/%d/%Y').strftime("%Y-%m-%d")
                    except:
                        pass
                    try:
                        value = datetime.datetime.strptime(value, '%m/%d/%y').strftime("%Y-%m-%d")
                    except:
                        pass
                    processed[key] = value
        actions.append({
            "_op_type": "index",
            "_index": "reddit_ads",
            "_id": obj["id"],
            "_source": {
                "obj": obj,
                "processed": processed,
                "meta": {
                    "last_indexed": datetime.datetime.now(datetime.timezone.utc)
                }
            }
        })
    helpers.bulk(es, actions)
    logger.info(' - '.join(['ADS INDEXED', str(len(actions))]))

    # return true
    return True
