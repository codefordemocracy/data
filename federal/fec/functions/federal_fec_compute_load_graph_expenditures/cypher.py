# Note that Neo4j has multiple constraints
# CREATE CONSTRAINT ON (a:Candidate) ASSERT a.cand_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Committee) ASSERT a.cmte_id IS UNIQUE;
# CREATE CONSTRAINT ON (a:Expenditure) ASSERT (a.file_num, a.tran_id) IS NODE KEY;
# CREATE CONSTRAINT ON (a:Payee) ASSERT a.name IS UNIQUE;

def merge_ind_exp_new_with_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Candidate {cand_id: i.cand_id}) "
           "MERGE (r:Expenditure {type: 'independent', file_num: i.file_num, tran_id: i.tran_id}) "
           "MERGE (a)-[x:SPENT]->(r)-[y:IDENTIFIES]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.transaction_amt=i.transaction_amt, r.sup_opp=i.sup_opp, r.purpose=i.purpose, r.amndt_ind = i.amndt_ind, r.image_num=i.image_num "
           "MERGE (p:Payee {name: i.payee}) "
           "MERGE (r)-[q:PAID]->(p) "
           "ON CREATE SET q.uuid = apoc.create.uuid() "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid() "
           "MERGE (a)-[o:TARGETS]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
           batch=batch)

def merge_ind_exp_new_without_date(tx, batch):
    tx.run("UNWIND $batch AS i "
           "MERGE (a:Committee {cmte_id: i.cmte_id}) "
           "MERGE (b:Candidate {cand_id: i.cand_id}) "
           "MERGE (r:Expenditure {type: 'independent', file_num: i.file_num, tran_id: i.tran_id}) "
           "MERGE (a)-[x:SPENT]->(r)-[y:IDENTIFIES]->(b) "
           "ON CREATE SET x.uuid = apoc.create.uuid(), y.uuid = apoc.create.uuid() "
           "SET r.transaction_amt=i.transaction_amt, r.sup_opp=i.sup_opp, r.purpose=i.purpose, r.amndt_ind = i.amndt_ind, r.image_num=i.image_num "
           "MERGE (p:Payee {name: i.payee}) "
           "MERGE (r)-[q:PAID]->(p) "
           "ON CREATE SET q.uuid = apoc.create.uuid() "
           "MERGE (a)-[o:TARGETS]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
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
           "SET r.datetime=datetime({ year: i.year, month: i.month, day: i.day, hour: i.hour, minute: i.minute, timezone: 'Z' }), r.transaction_amt=i.transaction_amt, r.sup_opp=i.sup_opp, r.purpose=i.purpose, r.amndt_ind = i.amndt_ind, r.image_num=i.image_num "
           "MERGE (p:Payee {name: i.payee}) "
           "MERGE (r)-[q:PAID]->(p) "
           "ON CREATE SET q.uuid = apoc.create.uuid() "
           "MERGE (c:Day {year: i.year, month: i.month, day: i.day}) "
           "MERGE (r)-[s:HAPPENED_ON]->(c) "
           "ON CREATE SET s.uuid = apoc.create.uuid() "
           "MERGE (a)-[o:TARGETS]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
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
           "SET r.transaction_amt=i.transaction_amt, r.sup_opp=i.sup_opp, r.purpose=i.purpose, r.amndt_ind = i.amndt_ind, r.image_num=i.image_num "
           "MERGE (p:Payee {name: i.payee}) "
           "MERGE (r)-[q:PAID]->(p) "
           "ON CREATE SET q.uuid = apoc.create.uuid() "
           "MERGE (a)-[o:TARGETS]->(b) "
           "ON CREATE SET o.uuid = apoc.create.uuid() ",
           batch=batch)
