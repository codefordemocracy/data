# Note that Neo4j has multiple constraints
# CREATE CONSTRAINT ON (a:Tweeter) ASSERT a.user_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Tweet) ASSERT a.tweet_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Hashtag) ASSERT a.text IS UNIQUE;
# CREATE CONSTRAINT ON (a:Annotation) ASSERT (a.type, a.text) IS NODE KEY;
# CREATE CONSTRAINT ON (a:Link) ASSERT a.url IS UNIQUE;

def merge_tweets(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Tweet {tweet_id: i.tweet_id}) "
           "SET a.datetime = datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), a.summary = i.summary, a.url = i.url "
           "MERGE (b:Tweeter {user_id: i.user_id}) "
           "SET b.username = i.username "
           "MERGE (a)-[r:PUBLISHED_BY]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid() "
           "MERGE (c:Day {year: i.est_year, month: i.est_month, day: i.est_day}) "
           "MERGE (a)-[s:PUBLISHED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid()",
           batch=batch)

def merge_tweeters(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Tweeter {user_id: i.user_id}) "
           "SET a.username = i.username, a.name = i.name, a.verified = i.verified",
           batch=batch)

def merge_hashtags(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Tweet {tweet_id: i.tweet_id}) "
           "MERGE (b:Hashtag {text: i.hashtag}) "
           "MERGE (a)-[r:MENTIONS]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_mentions(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Tweet {tweet_id: i.tweet_id}) "
           "MERGE (b:Tweeter {user_id: i.user_id}) "
           "ON CREATE SET b.username = i.username "
           "MERGE (a)-[r:MENTIONS]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_annotations(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Tweet {tweet_id: i.tweet_id}) "
           "MERGE (b:Annotation {type: i.type, text: i.text}) "
           "MERGE (a)-[r:MENTIONS {probability: i.probability}]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_links(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Tweet {tweet_id: i.tweet_id}) "
           "MERGE (b:Link {url: i.url}) "
           "MERGE (a)-[r:MENTIONS]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_quotes(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Tweet {tweet_id: i.tweet_id}) "
           "SET a.is_quote = 1 "
           "MERGE (b:Tweet {tweet_id: i.quote_tweet_id}) "
           "MERGE (a)-[r:QUOTED]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_replies(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Tweet {tweet_id: i.tweet_id}) "
           "SET a.is_reply = 1 "
           "MERGE (b:Tweet {tweet_id: i.reply_tweet_id}) "
           "MERGE (a)-[r:REPLIED_TO]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_retweets(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Tweet {tweet_id: i.tweet_id}) "
           "SET a.is_retweet = 1 "
           "MERGE (b:Tweet {tweet_id: i.retweet_id}) "
           "MERGE (a)-[r:RETWEETED]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)
