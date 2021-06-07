import logging
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import pubsub
from elasticsearch import Elasticsearch
import json
from furl import furl
import requests
from newspaper import Article
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
scraperapi_api_key = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/scraperapi_api_key/versions/1"}).payload.data.decode()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
db = firestore.Client()
publisher = pubsub.PublisherClient()

# utility function for basic newspaper call
def newspaperbasic(url):
    logger.info(' - '.join(['NEWSPAPER BASIC', url]))
    return Article(url, request_timeout=10)

# utility function for newspaper call with proxy
def newspaperproxy(url):
    logger.info(' - '.join(['NEWSPAPER PROXY', url]))
    return Article('http://api.scraperapi.com?key=' + scraperapi_api_key + '&url=' + url, request_timeout=70)

# utility function for newspaper call with proxy and javascript enabled
def newspaperproxyjs(url):
    logger.info(' - '.join(['NEWSPAPER PROXY JS', url]))
    return Article('http://api.scraperapi.com?key=' + scraperapi_api_key + '&render=true&url=' + url, request_timeout=70)

# utility function to strip URLs of the proxy string
def parse_proxy(url):
    if '&url=' in url:
        return url.split('&url=',1)[1]
    return url

# utility to unshorten url
def unshorten_url(url):
    if len(url) < 30:
        try:
            url = requests.head(url, allow_redirects=True, timeout=5).url
        except:
            pass
    return url

# utility function to strip URLs of the schema, parameters, and www
def strip_url(url):
    if "youtube.com/watch" in url:
        f = furl(url)
        try:
            v = f.args['v']
            url = f.remove(args=True, fragment=True).url
            url = furl(url).add(args={'v':v}).url
        except:
            pass
    else:
        url = furl(url).remove(args=True, fragment=True).url
    if '://www.' in url:
        url = url.split('://www.',1)[1]
    elif '://' in url:
        url = url.split('://',1)[1]
    return url

# downloads 5 articles and sends to ElasticSearch if the article has not already been scraped
# updates the Cloud Firestore list of scraped articles
# calls self with remaining articles
def news_articles_ingest_get_articles(message, context):

    # get the domain from the Pub/Sub message
    domain = message['attributes']['domain']

    # set up Firestore refs
    stored_ref = db.collection('news').document('articles').collection('scraped')
    failed_ref = db.collection('news').document('articles').collection('404')
    queue_ref = db.collection('news').document('queues').collection('crawler').document(domain)

    # get the information for scraping
    chunk = queue_ref.get().to_dict()
    articles = json.loads(chunk['articles'])
    scraper = chunk['scraper']
    logger.info(' - '.join(['INFO', 'total article urls found in queue', str(len(articles)), domain, scraper]))

    # pop off 5 articles to scrape and leave the remaining
    if len(articles) > 5:
        remaining = articles[5:]
        articles = articles[:5]
    else:
        remaining = []

    # check that the scraper is valid
    if scraper not in ['newspaperbasic','newspaperproxy','newspaperproxyjs']:
        logger.error(' - '.join(['INVALID SCRAPER', scraper]))
        return False

    loop = 0
    for url in articles:
        # throw out articles that are not actually from the domain
        if domain not in url:
            logger.error(' - '.join(['ARTICLE NOT IN DOMAIN', url, domain]))
            continue
        url = unshorten_url(url)
        stripped_url = strip_url(url)
        # check that the article has not already been scraped or failed
        stored = stored_ref.where('url', '==', stripped_url).get()
        failed = failed_ref.where('url', '==', stripped_url).get()
        if len(list(stored)) > 0:
            logger.error(' - '.join(['ARTICLE ALREADY SCRAPED', url]))
            continue
        elif len(list(failed)) > 0:
            logger.error(' - '.join(['ARTICLE ALREADY FAILED', url]))
            continue
        else:
            if loop > 0:
                # a delay to space out requests to websites
                logger.info(' - '.join(['STATUS', 'begin waiting for 3 seconds']))
                time.sleep(3)
                logger.info(' - '.join(['STATUS', 'finish waiting for 3 seconds']))
            # get the article using the right scraper
            if scraper == 'newspaperbasic':
                article = newspaperbasic(url)
            elif scraper == 'newspaperproxy':
                article = newspaperproxy(url)
            elif scraper == 'newspaperproxyjs':
                article = newspaperproxyjs(url)
            loop += 1
            # download and parse
            try:
                article.download()
                article.parse()
            except Exception as e:
                if '404' in str(e):
                    # prepare the record for Firestore
                    record = {
                        "url": stripped_url,
                        "datetime": datetime.datetime.now(datetime.timezone.utc)
                    }
                    # add the article to Firestore list of failed urls
                    failed_ref.add(record)
                logger.error(' - '.join(['ARTICLE DOWNLOAD FAILED', url, str(e)]))
                continue
            # create the document for ElasticSearch
            doc = {
                "extracted": {
                    "url": parse_proxy(article.url),
                    "title": article.title,
                    "date": article.publish_date,
                    "authors": article.authors,
                    "text": article.text,
                    "metadata": {
                        "keywords": article.meta_keywords,
                        "description": article.meta_description,
                        "language": article.meta_lang,
                        "url": article.canonical_link,
                        "dump": article.meta_data
                    },
                    "source": {
                        "url": parse_proxy(article.source_url),
                        "sitename": article.meta_site_name
                    }
                },
                "meta": {
                    "scraper": scraper,
                    "last_indexed": datetime.datetime.now(datetime.timezone.utc)
                }
            }
            # store doc in ElasticSearch and grab id
            response = es.index(index='news_articles',body=doc)
            logger.info(' - '.join(['COMPLETED', 'article added to ElasticSearch', response['_id'], parse_proxy(article.url)]))
            # prepare the record for Firestore
            record = {
                "url": stripped_url,
                "scraper": scraper,
                "datetime": datetime.datetime.now(datetime.timezone.utc)
            }
            # add the article to Firestore using the returned ElasticSearch id
            stored_ref.document(response['_id']).set(record)
            logger.info(' - '.join(['COMPLETED', 'article added to master Firestore list', stripped_url]))

    logger.info(' - '.join(['COMPLETED', 'articles processed', str(len(articles))]))

    if len(remaining) > 0:
        # update Firestore queue with remaining articles
        chunk = {
            "articles": json.dumps(remaining),
            "scraper": scraper
        }
        queue_ref.set(chunk)
        # sends a message to Pub/Sub to call self with remaining articles
        topic = 'projects/' + gcp_project_id + '/topics/news_articles_ingest_get_articles'
        publisher.publish(topic, b'extract articles from this domain', domain=domain)
        logger.info(' - '.join(['COMPLETED', 'domain sent to news_articles_ingest_get_articles queue', str(len(remaining)), domain, scraper]))
    else:
        # delete the Firestore queue doc
        queue_ref.delete()
        logger.info(' - '.join(['COMPLETED', 'all articles from domain processed', domain, scraper]))

    # return the number of processed articles
    return len(articles)
