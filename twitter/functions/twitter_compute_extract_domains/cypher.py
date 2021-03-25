# Note that Neo4j has multiple constraints
# CREATE CONSTRAINT ON (a:Domain) ASSERT a.host IS UNIQUE;
# CREATE CONSTRAINT ON (a:Link) ASSERT a.url IS UNIQUE;

def get_links(tx):
    urls = tx.run("MATCH (a:Link) "
                  "WHERE NOT (a)-[:ASSOCIATED_WITH]->(:Domain) "
                  "RETURN a.url AS url "
                  "LIMIT 1000")
    return urls.data()

def merge_link_domain(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MATCH (a:Link {url: i.url}) "
           "MERGE (b:Domain {host: i.domain}) "
           "MERGE (a)-[r:ASSOCIATED_WITH]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)
