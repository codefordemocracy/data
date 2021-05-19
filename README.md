# Data

This repository contains the code for ingesting the data used by Code for Democracy's platform.

## Cloud Services

We use a variety of cloud services in order to manage our data ingestion pipeline:

* **Scrapy Cloud**: runs scrapy spiders
* **Google Cloud Scheduler**: schedules functions
* **Google Cloud Functions**: executes the data loading tasks
* **Google Cloud Pub/Sub**: coordinates the Cloud Functions
* **Google Cloud Firestore**: stores queues used in ingestion
* **Google Cloud Storage**: stores downloaded raw files

## Databases

The data is stored in a few different databases:

* **Google Cloud BigQuery**: stores the tabular data
* **Elasticsearch**: stores the scraped documents
* **Neo4j**: stores the knowledge graph

## Functions

The work of downloading, transforming, and loading datasets is handled by these serverless functions:

### News

#### Sources

* **news_sources_ingest_run_spiders**: kicks off the allsides and mediabiasfactcheck spiders on Scrapy Cloud
* **news_sources_ingest_check_spiders**: polls the spiders every 60 seconds to check for completion
* **news_sources_ingest_get_crawls**: downloads the data from the crawls from Scrapy Cloud and queues records for processing
* **news_sources_ingest_verify_domain**: checks that each domain is still alive and adds to Cloud Firestore
* **news_sources_compute_load_graph**: loads sources into Neo4j
* **news_sources_compute_merge_domains**: connects domains with sources

#### Articles

* **news_articles_ingest_queue_domains**: queues list of active domains from Cloud Firestore
* **news_articles_ingest_get_paper**: queues article urls for each domain and logs the scraper used in Firestore
* **news_articles_ingest_get_articles**: scrapes articles and sends to ElasticSearch if the article is new, updates the Cloud Firestore list of articles
* **news_articles_ingest_process_stragglers**: queues remaining article urls for domains that errored out
* **news_articles_ingest_queue_duplicates**: iterates through ElasticSearch and queues batches of duplicate articles
* **news_articles_ingest_delete_duplicate**: deletes each duplicate document from ElasticSearch and Firestore
* **news_articles_ingest_get_url**: function for scraping just one url, called when urls are found in other pipelines

### Facebook

* **facebook_ingest_get_ads**: indexes Facebook ads into ElasticSearch
* **facebook_compute_load_ads**: takes Facebook ads from ElasticSearch and load into Neo4j

### Reddit

* **reddit_ingest_get_ads**: indexes Reddit ads into ElasticSearch

### Twitter

* **twitter_ingest_queue_get**: triggers the get functions and deletes users with many errors
* **twitter_ingest_primary_get_users**: get hydrated user information of a list of user ids and update Firestore, Elasticsearch, and Table Storage
* **twitter_ingest_primary_get_timeline**: get timeline tweets from user id and update Firestore, Table Storage, and ElasticSearch
* **twitter_ingest_primary_get_tweets**: get hydrated tweet information of a list of tweet ids and update Firestore, Table Storage, and ElasticSearch
* **twitter_ingest_secondary_get_users**: get hydrated user information of a list of user ids and update Elasticsearch and Table Storage
* **twitter_compute_queue_users**: queue loading tweets from primary users
* **twitter_compute_load_graph**: loads tweets from Elasticsearch into Neo4j
* **twitter_compute_catchup_graph**: searches Neo4j for tweets that errored out and sends for reloading
* **twitter_compute_extract_domains**: extracts domains from links

### Federal

#### FEC

* **federal_fec_ingest_queue_download**: sends FEC file urls to be downloaded
* **federal_fec_ingest_download_zip**: downloads FEC zip file into Google Cloud Storage
* **federal_fec_ingest_queue_import**: sends FEC file paths from Google Cloud Storage to be imported
* **federal_fec_ingest_import_bigquery**: imports the data from Cloud Storage into BigQuery
* **federal_fec_ingest_create_master_tables**: creates the master candidates, committees, and contributions tables
* **federal_fec_ingest_get_reports**: gets reports from FEC API and loads them into BigQuery
* **federal_fec_ingest_get_financials**: gets financials from FEC API and loads them into BigQuery
* **federal_fec_compute_load_graph**: loads the FEC data into Neo4j
* **federal_fec_ingest_unzip_gcs**: automatically unzips .zip files in Google Cloud Storage

#### House

* **federal_house_lobbying_ingest_get_disclosures**: indexes House lobbying disclosures into ElasticSearch
* **federal_house_lobbying_ingest_get_contributions**: indexes House lobbying contributions into ElasticSearch

#### Senate

* **federal_senate_lobbying_ingest_get_disclosures**: indexes Senate lobbying disclosures into ElasticSearch
* **federal_senate_lobbying_ingest_get_contributions**: indexes Senate lobbying contributions into ElasticSearch

## Principles

Except for nodes that represent extracted information (ie: domains), each dataset is loaded into Neo4j using a set of labels that are unique to the dataset so that it can be restructured or ripped out without damaging the rest of the knowledge graph.
