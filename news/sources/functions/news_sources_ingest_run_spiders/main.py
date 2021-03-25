import logging
from scrapinghub import ScrapinghubClient
from google.cloud import secretmanager
from google.cloud import pubsub

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

# run the spiders on scrapy cloud and return the job ids
def news_sources_ingest_run_spiders(message, context):

	# start the allsides spider on scrapy cloud
	allsides = project.jobs.run('allsides')
	logger.info(' - '.join(['STATUS', allsides.metadata.get('spider'), allsides.key, 'started']))

	# start the mediabiasfactcheck spider on scrapy cloud
	mediabiasfactcheck = project.jobs.run('mediabiasfactcheck')
	logger.info(' - '.join(['STATUS', mediabiasfactcheck.metadata.get('spider'), mediabiasfactcheck.key, 'started']))

	# send a message to Pub/Sub that the spider has been started
	topic = 'projects/' + gcp_project_id + '/topics/news_sources_ingest_check_spiders'
	publisher.publish(topic, b'spiders are running', allsides=allsides.key, mediabiasfactcheck=mediabiasfactcheck.key)
	logger.info(' - '.join(['STATUS', 'message sent to news_sources_ingest_check_spiders queue']))

	# return the scrapy cloud job keys
	return {"allsides": allsides.key, "mediabiasfactcheck": mediabiasfactcheck.key}
