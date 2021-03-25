## Theory

There are 2 concentric networks of Twitter users that we track:

* **Primary**: Core users that are manually loaded because they are relevant to US politics. We index all tweets from their timelines.
* **Secondary**: Users that were quoted, replied to, or retweeted by primary users.

## Architecture

Tweets are stored in ElasticSearch, Firestore (as a queue for unhydrated tweets), and Table Storage. Users are stored in Elasticsearch, Firestore (includes hydrated primary users and as a queue for secondary users), and Table Storage (all hydrated in one table for lookup). Relationships between users and tweets are only stored in Elasticsearch.

## Errors

Twitter users can become deactivated or suspended, in which case they clog up the queues and consume API hits without giving us any usable data. In order to clean up these users, secondary users with many errors are deleted from Firestore. They are retained in Elasticsearch and classified as *error* users in Table Storage.
