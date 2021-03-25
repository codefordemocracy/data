import logging
from scrapinghub import ScrapinghubClient
from google.cloud import secretmanager
from google.cloud import pubsub
import time
import json

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
scrapy_api_key = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/scrapy_api_key/versions/1"}).payload.data.decode()
scrapy_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/scrapy_project_id/versions/1"}).payload.data.decode()
gcp_project_id = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/gcp_project_id/versions/1"}).payload.data.decode()

# connect to resources
publisher = pubsub.PublisherClient()

# scrapy cloud settings
client = ScrapinghubClient(scrapy_api_key)
project = client.get_project(int(scrapy_project_id))

# utility function to check if job is completed
def check_spider_completion(key):
	job = project.jobs.get(key)
	logger.info(' - '.join(['STATUS', job.metadata.get('spider'), job.key, job.metadata.get('state')]))
	if job.metadata.get('state') == 'finished':
		logger.info(' - '.join(['COMPLETED', job.metadata.get('spider'), job.key, json.dumps(job.metadata.get('scrapystats'))]))
		return True
	return False

# check the spider runs on scrapy cloud and return the job ids if both are finished
def news_sources_ingest_check_spiders(message, context):

	# get job keys from the Pub/Sub message
	allsides = message['attributes']['allsides']
	mediabiasfactcheck = message['attributes']['mediabiasfactcheck']

	complete = False

	if check_spider_completion(allsides) and check_spider_completion(mediabiasfactcheck):
		# send a message to Pub/Sub that the data is ready to be downloaded
		complete = True
		logger.info(' - '.join(['STATUS', 'spider runs completed']))
		topic = 'projects/' + gcp_project_id + '/topics/news_sources_ingest_get_crawls'
		publisher.publish(topic, b'spiders finished', allsides=allsides, mediabiasfactcheck=mediabiasfactcheck)
		logger.info(' - '.join(['STATUS', 'message sent to news_sources_ingest_get_crawls queue']))
	else:
		# wait 60 seconds and send a message to Pub/Sub to check the spiders again
		logger.info(' - '.join(['STATUS', 'spiders are still running']))
		logger.info(' - '.join(['STATUS', 'begin waiting for 60 seconds']))
		time.sleep(60)
		logger.info(' - '.join(['STATUS', 'finish waiting for 60 seconds']))
		topic = 'projects/' + gcp_project_id + '/topics/news_sources_ingest_check_spiders'
		publisher.publish(topic, b'spiders are running', allsides=allsides, mediabiasfactcheck=mediabiasfactcheck)
		logger.info(' - '.join(['STATUS', 'message resent to news_sources_ingest_check_spiders queue']))

	# return the scrapy cloud job keys and completion flag
	return {"allsides": allsides, "mediabiasfactcheck": mediabiasfactcheck, "complete": complete}
