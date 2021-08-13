from azure.cosmosdb.table.models import Entity
import datetime
import json

# utility formatting function
def format_naked_tweet(id):
    return {
        "id": id,
        "hydrated": False,
        "skipped": False,
        "last_updated": datetime.datetime.now(datetime.timezone.utc)
    }

# utility formatting function
def format_naked_user(user_id):
    return {
        "id": user_id,
        "hydrated": False,
        "tweets": {
            "min_id": "-1",
            "max_id": "-1"
        },
        "last_updated": datetime.datetime.now(datetime.timezone.utc)
    }

# utility formatting function
def format_naked_relationship(record):
    # record["last_updated"] = datetime.datetime.now(datetime.timezone.utc)
    return record

# create the tweet doc for elasticsearch
def parse_tweets(tweet):
    # set up objects to return
    tweets = []
    users = []
    retweets = []
    quotes = []
    replies = []
    # process tweet
    main = {
        "id": tweet.id_str,
        "user_id": tweet.user.id_str,
        "user_screen_name": tweet.user.screen_name,
        "obj": {
            "id": tweet.id_str,
            "created_at": tweet.created_at,
            "text": tweet.full_text,
            "source": tweet.source,
            "retweet_count": tweet.retweet_count,
            "favorite_count": tweet.favorite_count,
            "entities": json.dumps(tweet.entities),
            "coordinates": tweet.coordinates,
            "lang": tweet.lang
        },
        "hydrated": True,
        "last_hydrated": datetime.datetime.now(datetime.timezone.utc),
        "last_updated": datetime.datetime.now(datetime.timezone.utc)
    }
    try:
        main["obj"]["extended_entities"] = json.dumps(tweet.extended_entities)
    except:
        pass
    try:
        main["obj"]["place"] = json.dumps({
            "id": tweet.place.id,
            "url": tweet.place.url,
            "place_type": tweet.place.place_type,
            "name": tweet.place.name,
            "full_name": tweet.place.full_name,
            "country_code": tweet.place.country_code,
            "country": tweet.place.country,
            "bounding_box": {
                "type": tweet.place.bounding_box.type,
                "coordinates": tweet.place.bounding_box.coordinates,
            },
            "attributes": tweet.place.attributes
        })
    except:
        pass
    # process retweet
    try:
        main["retweet"] = {
            "id": tweet.retweeted_status.id_str,
            "user_id": tweet.retweeted_status.user.id_str
        }
        main["is_retweet"] = True
    except:
        main["is_retweet"] = False
    if main["is_retweet"] is True:
        tweets.append(format_naked_tweet(main["retweet"]["id"]))
        users.append(format_naked_user(main["retweet"]["user_id"]))
        main["obj"]["text"] = tweet.retweeted_status.full_text
        retweet = format_naked_relationship({
            "source": {
                "user": tweet.user._json,
                "obj": main["obj"]
                },
            "target": {
                "user": tweet.retweeted_status.user._json,
                "obj": tweet.retweeted_status._json
                },
            "meta": {
                "in_graph": False,
                "last_updated": datetime.datetime.now(datetime.timezone.utc)
            }
        })
        retweet['source']['user']['created_at'] = tweet.user.created_at
        retweet['source']['obj']['entities'] = tweet.entities
        retweet['target']['user']['created_at'] = tweet.retweeted_status.user.created_at
        retweet['target']['obj']['created_at'] = tweet.retweeted_status.created_at
        retweets.append(retweet)
    # process quote
    try:
        main["quote"] = {
            "id": tweet.quoted_status.id_str,
            "user_id": tweet.quoted_status.user.id_str
        }
        main["is_quote"] = True
    except:
        main["is_quote"] = False
    if main["is_quote"] is True:
        tweets.append(format_naked_tweet(main["quote"]["id"]))
        users.append(format_naked_user(main["quote"]["user_id"]))
        quotes.append(format_naked_relationship({
            "source": main["obj"]["id"],
            "target": main["quote"]["id"],
            "created_at": main["obj"]["created_at"],
            "last_updated": datetime.datetime.now(datetime.timezone.utc)
        }))
    # process reply
    if tweet.in_reply_to_status_id_str is not None:
        main["reply"] = {
            "id": tweet.in_reply_to_status_id_str,
            "user_id": tweet.in_reply_to_user_id_str
        }
        main["is_reply"] = True
    else:
        main["is_reply"] = False
    if main["is_reply"] is True:
        tweets.append(format_naked_tweet(main["reply"]["id"]))
        users.append(format_naked_user(main["reply"]["user_id"]))
        replies.append(format_naked_relationship({
            "source": main["obj"]["id"],
            "target": main["reply"]["id"],
            "created_at": main["obj"]["created_at"],
            "last_updated": datetime.datetime.now(datetime.timezone.utc)
        }))
    # return all the objects
    return {
        "main": main,
        "tweets": tweets,
        "users": users,
        "retweets": retweets,
        "quotes": quotes,
        "replies": replies
    }

# create the tweet entity for table storage
def create_tweet_entity(record):
    row = Entity()
    row.PartitionKey = record["user_id"]
    row.RowKey = record["id"]
    row.user_screen_name = record["user_screen_name"]
    row.created_at = record["obj"]["created_at"]
    row.text = record["obj"]["text"]
    row.source = record["obj"]["source"]
    row.retweet_count = record["obj"]["retweet_count"]
    row.favorite_count = record["obj"]["favorite_count"]
    row.entities = record["obj"]["entities"]
    row.coordinates = str(record["obj"]["coordinates"])
    row.lang = record["obj"]["lang"]
    row.is_retweet = record["is_retweet"]
    row.is_quote = record["is_quote"]
    row.is_reply = record["is_reply"]
    row.last_hydrated = record["last_hydrated"]
    return row
