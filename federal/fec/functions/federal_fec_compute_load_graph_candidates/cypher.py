# Note that Neo4j has multiple constraints
# CREATE CONSTRAINT ON (a:Candidate) ASSERT a.cand_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:State) ASSERT a.abbreviation IS UNIQUE;
# CREATE CONSTRAINT ON (a:Party) ASSERT a.abbreviation IS UNIQUE;
# CREATE CONSTRAINT ON (a:Race) ASSERT (a.type, a.election_yr, a.office, a.office_st, a.office_district) IS NODE KEY;

def merge_node_candidate(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Candidate {cand_id: i.cand_id}) "
           "SET a.cand_name = i.cand_name, a.cand_pty_affiliation = i.cand_pty_affiliation, a.cand_election_yr = i.cand_election_yr, a.cand_office_st = i.cand_office_st, a.cand_office = i.cand_office, a.cand_office_district = i.cand_office_district, a.cand_ici = i.cand_ici "
           "MERGE (b:State {abbreviation: i.cand_office_st}) "
           "MERGE (a)-[r:RUNNING_IN]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_candidate_party(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Candidate {cand_id: i.cand_id}) "
           "MERGE (b:Party {abbreviation: i.cand_pty_affiliation}) "
           "MERGE (a)-[r:ASSOCIATED_WITH]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_candidate_race(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Candidate {cand_id: i.cand_id}) "
           "MERGE (b:Race {type: 'federal', election_yr: i.cand_election_yr, office_st: i.cand_office_st, office: i.cand_office, office_district: i.cand_office_district}) "
           "MERGE (a)-[r:RUNNING_FOR]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid() "
           "MERGE (c:State {abbreviation: i.cand_office_st}) "
           "MERGE (b)-[s:ASSOCIATED_WITH]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_candidate_committee(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Candidate {cand_id: i.cand_id}) "
           "MERGE (a)-[r:ASSOCIATED_WITH {subtype: 'linkage', linkage_id: i.linkage_id}]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid() "
           "SET r.cand_election_yr = i.cand_election_yr",
           batch=batch)
