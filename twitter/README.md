## Theory

There are 2 concentric networks of Twitter users that we track:

* **Primary**: Core users that are manually loaded because they are relevant to US politics. We index all tweets from their timelines.
* **Secondary**: Users and tweets that were quoted, replied to, or retweeted by primary users.

## Architecture

Tweets are stored in ElasticSearch and Firestore (as a queue for unhydrated tweets). Users are stored in Elasticsearch.
