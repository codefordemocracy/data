# Note that Neo4j has multiple constraints
# CREATE CONSTRAINT ON (a:Committee) ASSERT a.cmte_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Party) ASSERT a.abbreviation IS UNIQUE;
# CREATE CONSTRAINT ON (a:Employer) ASSERT a.name IS UNIQUE;
# CREATE CONSTRAINT ON (a:Candidate) ASSERT a.cand_id IS UNIQUE;

def merge_node_committee(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "SET a.cmte_nm = i.cmte_nm, a.cmte_dsgn = i.cmte_dsgn, a.cmte_tp = i.cmte_tp, a.cmte_pty_affiliation = i.cmte_pty_affiliation, a.org_tp = i.org_tp, a.connected_org_nm = i.connected_org_nm",
           batch=batch)

def merge_rel_committee_party(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Party {abbreviation: i.cmte_pty_affiliation}) "
           "MERGE (a)-[r:ASSOCIATED_WITH]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_committee_employer(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Employer {name: i.connected_org_nm}) "
           "MERGE (a)-[r:ASSOCIATED_WITH]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_committee_candidate(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Candidate {cand_id: i.cand_id}) "
           "MERGE (a)-[r:ASSOCIATED_WITH {subtype: 'linkage', linkage_id: i.linkage_id}]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid() "
           "SET r.cand_election_yr = i.cand_election_yr",
           batch=batch)
