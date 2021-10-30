# Note that Neo4j has multiple constraints
# CREATE CONSTRAINT ON (a:Candidate) ASSERT a.cand_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Committee) ASSERT a.cmte_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Contribution) ASSERT a.sub_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Donor) ASSERT (a.name, a.zip_code) IS NODE KEY;
# CREATE CONSTRAINT ON (a:Employer) ASSERT a.name IS UNIQUE;
# CREATE CONSTRAINT ON (a:Job) ASSERT a.name IS UNIQUE;
# CREATE CONSTRAINT ON (a:State) ASSERT a.abbreviation IS UNIQUE;
# CREATE CONSTRAINT ON (a:Zip) ASSERT a.zip_code IS UNIQUE;

def merge_rel_committee_contributed_to_with_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.source}) "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: toString(i.sub_id)}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid() "
           "MERGE (a)-[o:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
           batch=batch)

def merge_rel_committee_contributed_to_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.source}) "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: toString(i.sub_id)}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (a)-[o:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
           batch=batch)

def merge_rel_candidate_contributed_to_with_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Candidate {cand_id: i.source}) "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: toString(i.sub_id)}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid() "
           "MERGE (a)-[o:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
           batch=batch)

def merge_rel_candidate_contributed_to_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Candidate {cand_id: i.source}) "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: toString(i.sub_id)}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (a)-[o:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
           batch=batch)

def merge_rel_donor_ind_contributed_to_with_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Donor {name: i.name, zip_code: i.zip_code}) "
           "SET a.entity_tp = i.entity_tp, a.state = i.state, a.employer = i.employer, a.occupation = i.occupation "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: toString(i.sub_id)}) "
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
           "ON CREATE SET w.uuid = apoc.create.uuid() "
           "MERGE (a)-[o:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
           batch=batch)

def merge_rel_donor_ind_contributed_to_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Donor {name: i.name, zip_code: i.zip_code}) "
           "SET a.entity_tp = i.entity_tp, a.state = i.state, a.employer = i.employer, a.occupation = i.occupation "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: toString(i.sub_id)}) "
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
           "ON CREATE SET w.uuid = apoc.create.uuid() "
           "MERGE (a)-[o:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
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
           "MERGE (r:Contribution {sub_id: toString(i.sub_id)}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid() "
           "MERGE (d:Employer {name: i.name}) "
           "MERGE (a)-[t:ASSOCIATED_WITH]->(d) "
           "ON CREATE SET t.uuid = apoc.create.uuid() "
           "MERGE (a)-[o:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
           batch=batch)

def merge_rel_donor_org_contributed_to_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Donor {name: i.name, zip_code: i.zip_code}) "
           "SET a.entity_tp = i.entity_tp, a.state = i.state "
           "MERGE (b:Committee {cmte_id: i.target}) "
           "MERGE (r:Contribution {sub_id: toString(i.sub_id)}) "
           "MERGE (a)-[x:CONTRIBUTED_TO]->(r)-[y:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.transaction_amt=i.transaction_amt, r.amndt_ind = i.amndt_ind, r.rpt_tp=i.rpt_tp, r.transaction_pgi=i.transaction_pgi, r.transaction_tp=i.transaction_tp, r.image_num=i.image_num, r.file_num=i.file_num, r.tran_id=i.tran_id "
           "MERGE (d:Employer {name: i.name}) "
           "MERGE (a)-[t:ASSOCIATED_WITH]->(d) "
           "ON CREATE SET t.uuid = apoc.create.uuid() "
           "MERGE (a)-[o:CONTRIBUTED_TO]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
           batch=batch)
