# Note that Neo4j has multiple constraints
# CREATE CONSTRAINT ON (a:Candidate) ASSERT a.cand_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Committee) ASSERT a.cmte_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Contribution) ASSERT a.sub_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Expenditure) ASSERT (a.file_num, a.tran_id) IS NODE KEY;
# CREATE CONSTRAINT ON (a:Donor) ASSERT (a.name, a.zip_code) IS NODE KEY;
# CREATE CONSTRAINT ON (a:Race) ASSERT (a.type, a.election_yr, a.office, a.office_st, a.office_district) IS NODE KEY;
# CREATE CONSTRAINT ON (a:Party) ASSERT a.abbreviation IS UNIQUE;
# CREATE CONSTRAINT ON (a:Employer) ASSERT a.name IS UNIQUE;
# CREATE CONSTRAINT ON (a:Job) ASSERT a.name IS UNIQUE;
# CREATE CONSTRAINT ON (a:Payee) ASSERT a.name IS UNIQUE;
# CREATE CONSTRAINT ON (a:State) ASSERT a.abbreviation IS UNIQUE;
# CREATE CONSTRAINT ON (a:Zip) ASSERT a.zip_code IS UNIQUE;

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

def merge_rel_committee_associated_with(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Candidate {cand_id: i.cand_id}) "
           "MERGE (a)-[r:ASSOCIATED_WITH {subtype: 'linkage', linkage_id: i.linkage_id}]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid() "
           "SET r.cand_election_yr = i.cand_election_yr",
           batch=batch)

def merge_rel_committee_contributed_to_with_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.source}) "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: i.sub_id}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_committee_contributed_to_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.source}) "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: i.sub_id}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id",
           batch=batch)

def merge_rel_candidate_contributed_to_with_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Candidate {cand_id: i.source}) "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: i.sub_id}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_candidate_contributed_to_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Candidate {cand_id: i.source}) "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: i.sub_id}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id",
           batch=batch)

def merge_rel_donor_ind_contributed_to_with_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Donor {name: i.name, zip_code: i.zip_code}) "
           "SET a.entity_tp = i.entity_tp, a.state = i.state, a.employer = i.employer, a.occupation = i.occupation "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: i.sub_id}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid() "
           "MERGE (d:Employer {name: i.employer}) "
           "MERGE (r)-[t:ASSOCIATED_WITH]->(d) "
           "ON CREATE SET t.uuid = apoc.create.uuid() "
           "MERGE (a)-[u:ASSOCIATED_WITH]->(d) "
           "ON CREATE SET u.uuid = apoc.create.uuid() "
           "MERGE (e:Job {name: i.occupation}) "
           "MERGE (r)-[v:ASSOCIATED_WITH]->(e) "
           "ON CREATE SET v.uuid = apoc.create.uuid() "
           "MERGE (a)-[w:ASSOCIATED_WITH]->(e) "
           "ON CREATE SET w.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_donor_ind_contributed_to_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Donor {name: i.name, zip_code: i.zip_code}) "
           "SET a.entity_tp = i.entity_tp, a.state = i.state, a.employer = i.employer, a.occupation = i.occupation "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: i.sub_id}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (d:Employer {name: i.employer}) "
           "MERGE (r)-[t:ASSOCIATED_WITH]->(d) "
           "ON CREATE SET t.uuid = apoc.create.uuid() "
           "MERGE (a)-[u:ASSOCIATED_WITH]->(d) "
           "ON CREATE SET u.uuid = apoc.create.uuid() "
           "MERGE (e:Job {name: i.occupation}) "
           "MERGE (r)-[v:ASSOCIATED_WITH]->(e) "
           "ON CREATE SET v.uuid = apoc.create.uuid() "
           "MERGE (a)-[w:ASSOCIATED_WITH]->(e) "
           "ON CREATE SET w.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_donor_ind_state(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Donor {name: i.name, zip_code: i.zip_code}) "
           "MERGE (b:State {abbreviation: i.state}) "
           "MERGE (a)-[r:LIVES_IN]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_donor_ind_zip(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Donor {name: i.name, zip_code: i.zip_code}) "
           "MERGE (b:Zip {zip_code: i.zip_code}) "
           "MERGE (a)-[r:LIVES_IN]->(b) "
           "ON CREATE SET r.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_donor_org_contributed_to_with_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Donor {name: i.name, zip_code: i.zip_code}) "
           "SET a.entity_tp = i.entity_tp, a.state = i.state "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: i.sub_id}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid() "
           "MERGE (d:Employer {name: i.name}) "
           "MERGE (a)-[t:ASSOCIATED_WITH]->(d) "
           "ON CREATE SET t.uuid = apoc.create.uuid()",
           batch=batch)

def merge_rel_donor_org_contributed_to_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Donor {name: i.name, zip_code: i.zip_code}) "
           "SET a.entity_tp = i.entity_tp, a.state = i.state "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: i.sub_id}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (d:Employer {name: i.name}) "
           "MERGE (a)-[t:ASSOCIATED_WITH]->(d) "
           "ON CREATE SET t.uuid = apoc.create.uuid()",
           batch=batch)

def merge_ind_exp_new_with_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Candidate {cand_id: i.cand_id}) "
           "MERGE (r:Expenditure {type: 'independent', file_num: i.file_num, tran_id: i.tran_id}) "
           "MERGE (a)-[x:SPENT]->(r)-[y:IDENTIFIES]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.exp_amt=i.exp_amt, r.sup_opp=i.sup_opp, r.purpose=i.purpose, r.amndt_ind = i.amndt_ind, r.image_num=i.image_num "
           "MERGE (p:Payee {name: i.payee}) "
           "MERGE (r)-[q:PAID]->(p) "
           "ON CREATE SET q.uuid = apoc.create.uuid() "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid()",
           batch=batch)

def merge_ind_exp_new_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Candidate {cand_id: i.cand_id}) "
           "MERGE (r:Expenditure {type: 'independent', file_num: i.file_num, tran_id: i.tran_id}) "
           "MERGE (a)-[x:SPENT]->(r)-[y:IDENTIFIES]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.exp_amt=i.exp_amt, r.sup_opp=i.sup_opp, r.purpose=i.purpose, r.amndt_ind = i.amndt_ind, r.image_num=i.image_num "
           "MERGE (p:Payee {name: i.payee}) "
           "MERGE (r)-[q:PAID]->(p) "
           "ON CREATE SET q.uuid = apoc.create.uuid()",
           batch=batch)

def merge_ind_exp_amend_with_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MATCH (k:Expenditure {type: 'independent', file_num: i.prev_file_num, tran_id: i.tran_id}) "
           "DETACH DELETE k "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Candidate {cand_id: i.cand_id}) "
           "MERGE (r:Expenditure {type: 'independent', file_num: i.file_num, tran_id: i.tran_id}) "
           "MERGE (a)-[x:SPENT]->(r)-[y:IDENTIFIES]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.exp_amt=i.exp_amt, r.sup_opp=i.sup_opp, r.purpose=i.purpose, r.amndt_ind = i.amndt_ind, r.image_num=i.image_num "
           "MERGE (p:Payee {name: i.payee}) "
           "MERGE (r)-[q:PAID]->(p) "
           "ON CREATE SET q.uuid = apoc.create.uuid() "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid()",
           batch=batch)

def merge_ind_exp_amend_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MATCH (k:Expenditure {type: 'independent', file_num: i.prev_file_num, tran_id: i.tran_id}) "
           "DETACH DELETE k "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Candidate {cand_id: i.cand_id}) "
           "MERGE (r:Expenditure {type: 'independent', file_num: i.file_num, tran_id: i.tran_id}) "
           "MERGE (a)-[x:SPENT]->(r)-[y:IDENTIFIES]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.exp_amt=i.exp_amt, r.sup_opp=i.sup_opp, r.purpose=i.purpose, r.amndt_ind = i.amndt_ind, r.image_num=i.image_num "
           "MERGE (p:Payee {name: i.payee}) "
           "MERGE (r)-[q:PAID]->(p) "
           "ON CREATE SET q.uuid = apoc.create.uuid()",
           batch=batch)
