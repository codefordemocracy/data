# Note that Neo4j has multiple constraints
# CREATE CONSTRAINT ON (a:Source) ASSERT a.domain IS UNIQUE

def merge_node_source(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Source {domain: i.domain}) "
           "SET a.name = i.name, a.bias_score = i.bias_score, a.conspiracy_flag = i.conspiracy_flag, a.factually_questionable_flag = i.factually_questionable_flag, a.hate_group_flag = i.hate_group_flag, a.propaganda_flag = i.propaganda_flag, a.satire_flag = i.satire_flag",
           batch=batch)
