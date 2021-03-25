def get_tweets(tx):
    ids = tx.run("MATCH (a:Tweet) "
                 "WHERE a.datetime IS NULL "
                 "RETURN a.tweet_id AS tweet_id "
                 "ORDER BY ID(a) DESC "
                 "LIMIT 30000")
    return ids.data()
