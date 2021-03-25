# Note that Neo4j has multiple constraints
# CREATE CONSTRAINT ON (a:Domain) ASSERT a.host IS UNIQUE;
# CREATE CONSTRAINT ON (a:Source) ASSERT a.domain IS UNIQUE;

def get_sources(tx):
    domains = tx.run("MATCH (a:Source) "
                     "WITH a, rand() AS r "
                     "ORDER BY r "
                     "RETURN a.domain AS domain "
                     "LIMIT 1000")
    return domains.data()

def match_domains(tx, batch):
    pairs = tx.run("UNWIND $batch AS i "
                   "MATCH (a:Domain) "
                   "WHERE NOT (a)-[:ASSOCIATED_WITH]->(:Source) "
                   "AND (a.host = i.domain OR a.host CONTAINS '.' + i.domain) "
                   "RETURN a.host as host, i.domain as domain",
                   batch=batch)
    return pairs.data()

def merge_domain_source(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MATCH (a:Domain {host: i.host}) "
           "MATCH (b:Source {domain: i.domain}) "
           "MERGE (a)-[r:ASSOCIATED_WITH]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)
