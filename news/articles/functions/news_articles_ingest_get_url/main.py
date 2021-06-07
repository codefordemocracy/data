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

# connect to resources
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)
db = firestore.Client()

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

# downloads a single article and sends to ElasticSearch if the article has not already been scraped
def news_articles_ingest_get_url(message, context):

    # get the articles from the Pub/Sub message
    url = message['attributes']['url']
    url = furl(url).remove(args=True, fragment=True).url

    # set up Firestore refs
    stored_ref = db.collection('news').document('articles').collection('scraped')
    failed_ref = db.collection('news').document('articles').collection('404')

    articles = []
    articles.append(url)
    for url in articles:
        url = unshorten_url(url)
        stripped_url = strip_url(url)
        # check that the article has not already been scraped or failed
        stored = stored_ref.where('url', '==', stripped_url).get()
        failed = failed_ref.where('url', '==', stripped_url).get()
        if len(list(stored)) > 0:
            logger.info(' - '.join(['ARTICLE ALREADY SCRAPED', url]))
            continue
        elif len(list(failed)) > 0:
            logger.info(' - '.join(['ARTICLE ALREADY FAILED', url]))
            continue
        else:
            # TODO: get the article using the right scraper
            scraper = "newspaperbasic"
            article = newspaperbasic(url)
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
                elif '403' in str(e) or '429' in str(e):
                    scraper == 'newspaperproxy'
                    article = newspaperproxy(url)
                    try:
                        article.download()
                        article.parse()
                    except Exception as err:
                        if '403' in str(err) or '429' in str(err):
                            scraper == 'newspaperproxyjs'
                            article = newspaperproxyjs(url)
                            try:
                                article.download()
                                article.parse()
                            except Exception as error:
                                logger.error(' - '.join(['ARTICLE DOWNLOAD FAILED', scraper, url, str(error)]))
                        else:
                            logger.error(' - '.join(['ARTICLE DOWNLOAD FAILED', scraper, url, str(err)]))
                else:
                    logger.error(' - '.join(['ARTICLE DOWNLOAD FAILED', scraper, url, str(e)]))
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

    logger.info(' - '.join(['COMPLETED', 'url processed', url]))

    # return the number of processed articles
    return url
