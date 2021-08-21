# Note that Neo4j has multiple constraints
# CREATE CONSTRAINT ON (a:Ad) ASSERT a.id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Message) ASSERT (a.sha512, a.simhash) IS NODE KEY;
# CREATE CONSTRAINT ON (a:Page) ASSERT a.id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Buyer) ASSERT a.name IS UNIQUE;
# CREATE CONSTRAINT ON (a:State) ASSERT a.name IS UNIQUE;

def merge_ads_with_delivery_stop_time(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Ad {id: i.id}) "
           "SET a.creation_time=datetime({ year: i.creation_time.year, month: i.creation_time.month, day: i.creation_time.day, hour: i.creation_time.hour, minute: i.creation_time.minute, timezone: 'Z' }), \
           a.delivery_start_time=datetime({ year: i.delivery_start_time.year, month: i.delivery_start_time.month, day: i.delivery_start_time.day, hour: i.delivery_start_time.hour, minute: i.delivery_start_time.minute, timezone: 'Z' }), \
           a.delivery_stop_time=datetime({ year: i.delivery_stop_time.year, month: i.delivery_stop_time.month, day: i.delivery_stop_time.day, hour: i.delivery_stop_time.hour, minute: i.delivery_stop_time.minute, timezone: 'Z' }), \
           a.impressions_lower_bound = i.impressions_lower_bound, a.impressions_upper_bound = i.impressions_upper_bound, \
           a.spend_lower_bound = i.spend_lower_bound, a.spend_upper_bound = i.spend_upper_bound, \
           a.potential_reach_lower_bound = i.potential_reach_lower_bound, a.potential_reach_upper_bound = i.potential_reach_upper_bound, \
           a.creative_link_caption = i.creative_link_caption",
           batch=batch)

def merge_ads_without_delivery_stop_time(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Ad {id: i.id}) "
           "SET a.creation_time=datetime({ year: i.creation_time.year, month: i.creation_time.month, day: i.creation_time.day, hour: i.creation_time.hour, minute: i.creation_time.minute, timezone: 'Z' }), \
           a.delivery_start_time=datetime({ year: i.delivery_start_time.year, month: i.delivery_start_time.month, day: i.delivery_start_time.day, hour: i.delivery_start_time.hour, minute: i.delivery_start_time.minute, timezone: 'Z' }), \
           a.impressions_lower_bound = i.impressions_lower_bound, a.impressions_upper_bound = i.impressions_upper_bound, \
           a.spend_lower_bound = i.spend_lower_bound, a.spend_upper_bound = i.spend_upper_bound, \
           a.potential_reach_lower_bound = i.potential_reach_lower_bound, a.potential_reach_upper_bound = i.potential_reach_upper_bound, \
           a.creative_link_caption = i.creative_link_caption",
           batch=batch)

def merge_creation_days(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Ad {id: i.id}) "
           "MERGE (b:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (a)-[r:CREATED_ON]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_delivery_days(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Ad {id: i.id}) "
           "MERGE (b:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (a)-[r:DELIVERED_ON]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_messages(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Ad {id: i.id}) "
           "MERGE (b:Message {sha512: i.sha512, simhash: i.simhash}) "
           "MERGE (a)-[r:CONTAINS]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_pages(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Ad {id: i.id}) "
           "MERGE (b:Page {id: i.page_id}) "
           "SET b.name = i.page_name "
           "MERGE (a)-[r:PUBLISHED_BY]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_buyers(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Ad {id: i.id}) "
           "MERGE (b:Buyer {name: i.name}) "
           "MERGE (a)-[r:PAID_BY]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_pages_buyers(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Page {id: i.page_id}) "
           "MERGE (b:Buyer {name: i.buyer_name}) "
           "MERGE (a)-[r:ASSOCIATED_WITH]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_states(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Ad {id: i.id}) "
           "MERGE (b:State {name: i.state}) "
           "MERGE (a)-[r:TARGETS]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)
