import logging
from google.cloud import secretmanager
from google.cloud import firestore
from google.cloud import pubsub
import datetime
import json
import newspaper

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
scraperapi_api_key = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/scraperapi_api_key/versions/1"}).payload.data.decode()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
db = firestore.Client()
publisher = pubsub.PublisherClient()

# utility function for basic newspaper call
def newspaperbasic(domain):
    logger.info(' - '.join(['NEWSPAPER BASIC', domain]))
    return newspaper.build('http://' + domain, memoize_articles=False, request_timeout=10)

# utility function for newspaper call with proxy
def newspaperproxy(domain):
    logger.info(' - '.join(['NEWSPAPER PROXY', domain]))
    return newspaper.build('http://api.scraperapi.com?key=' + scraperapi_api_key + '&url=http://' + domain, memoize_articles=False, request_timeout=70, number_threads=2)

# utility function for newspaper call with proxy and javascript enabled
def newspaperproxyjs(domain):
    logger.info(' - '.join(['NEWSPAPER PROXY JS', domain]))
    return newspaper.build('http://api.scraperapi.com?key=' + scraperapi_api_key + '&render=true&url=http://' + domain, memoize_articles=False, request_timeout=70, number_threads=2)

# utility function to strip URLs of the proxy string
def parse_proxy(url):
    if '&url=' in url:
        return url.split('&url=',1)[1]
    return url

# creates paper from each domain, queues batches of article urls with a scraper setting, and notes the scraper used in Firestore
def news_articles_ingest_get_paper(message, context):

    # get domain from the Pub/Sub message
    domain = message['attributes']['domain']

    # set up Firestore refs
    domain_ref = db.collection('news').document('sources').collection('scraped').document(domain)
    queue_ref = db.collection('news').document('queues').collection('crawler').document(domain)
    doc = domain_ref.get().to_dict()

    # if there's not a saved scraper or it's Saturday morning, we re-check every source for scrapability
    # check potential scrapers for each domain by iterating from least to most expensive calls
    if 'scraper' not in doc or (datetime.datetime.now(datetime.timezone.utc).weekday() == 5 and datetime.datetime.now(datetime.timezone.utc).hour < 12):
        # basic newspaper call
        logger.info(' - '.join(['TRYING NEWSPAPER BASIC SCRAPER', domain]))
        paper = newspaperbasic(domain)
        scraper = 'newspaperbasic'
        if paper.size() == 0:
            # newspaper call with proxy
            logger.info(' - '.join(['TRYING NEWSPAPER PROXY SCRAPER', domain]))
            paper = newspaperproxy(domain)
            scraper = 'newspaperproxy'
            if paper.size() == 0:
                # newspaper call with proxy and javascript enabled
                logger.info(' - '.join(['TRYING NEWSPAPER PROXY JS SCRAPER', domain]))
                paper = newspaperproxyjs(domain)
                scraper = 'newspaperproxyjs'
                if paper.size() == 0:
                    # if none of those scrapers work, then we give up and skip the domain
                    logger.warning(' - '.join(['NO SUITABLE SCRAPER FOUND', domain]))
                    scraper = 'skip'
    else:
        # we use the stored scraper to scrape the site
        scraper = doc['scraper']
        if scraper == 'newspaperbasic':
            paper = newspaperbasic(domain)
        elif scraper == 'newspaperproxy':
            paper = newspaperproxy(domain)
        elif scraper == 'newspaperproxyjs':
            paper = newspaperproxyjs(domain)

    # get the number of articles
    size = 0
    if scraper != 'skip':
        size = paper.size()

    # send a message to Pub/Sub with the reference to a batch of article urls and the scraper
    if size > 0:
        # create the list of urls
        urls = []
        for article in paper.articles:
            url = parse_proxy(article.url)
            urls.append(url)
            logger.info(' - '.join(['STATUS', 'article found', url, scraper]))
        # add the urls and the scraper to the Firestore queue
        chunk = {
            "articles": json.dumps(urls),
            "scraper": scraper
        }
        queue_ref.set(chunk)
        logger.info(' - '.join(['STATUS', 'articles added to Firestore queue', str(size), domain, scraper]))
        # send a message to Pub/Sub telling get_articles that it's time to start scraping
        topic = 'projects/' + gcp_project_id + '/topics/news_articles_ingest_get_articles'
        publisher.publish(topic, b'extract articles from this domain', domain=domain)
        logger.info(' - '.join(['COMPLETED', 'domain sent to news_articles_ingest_get_articles queue', domain]))

    # update Firestore with the scraper
    doc['scraper'] = scraper
    domain_ref.set(doc, merge=True)
    logger.info(' - '.join(['COMPLETED', 'scraper settings updated', domain, scraper]))

    # return the domain, size, and scraper
    return {'domain': domain, 'size': size, 'scraper': scraper}
