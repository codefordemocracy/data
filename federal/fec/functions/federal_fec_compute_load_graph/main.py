import logging
from google.cloud import secretmanager
from neo4j import GraphDatabase
from google.cloud import bigquery
import pytz
import datetime
import pandas
import numpy as np
import cypher
import time
import random
import json

# format logs
formatter = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)
logging.basicConfig()
logger = logging.getLogger(__name__)

# get secrets
secrets = secretmanager.SecretManagerServiceClient()
neo4j_connection = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_connection/versions/1"}).payload.data.decode()
neo4j_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_username_data/versions/1"}).payload.data.decode()
neo4j_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/neo4j_password_data/versions/1"}).payload.data.decode()

# connect to resources
driver = GraphDatabase.driver(neo4j_connection, auth=(neo4j_username_data, neo4j_password_data))
client = bigquery.Client()

# helper function to generate a new job config
def gen_job_config():
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    return job_config

# helper function to get values string from array of values
def get_values_string(values):
    values_string = ""
    for value in values:
        values_string += "("
        try:
            values_string += "'" + value + "'"
        except:
            values_string += str(value)
        values_string += "), "
    values_string = values_string[:-2]
    return values_string

# helper function to get values string from array of rows
def get_values_string_rows(rows):
    values_string = ""
    for row in rows:
        values_string += "("
        for value in row:
            if value is None:
                values_string += "null"
            elif isinstance(value, list):
                array_string = json.dumps(value).replace("\\", "\\\\'").replace("'", "\\'")
                values_string += "'" + array_string + "'"
            else:
                try:
                    value = value.replace("\\", "\\\\'").replace("'", "\\'")
                    values_string += "'" + value + "'"
                except:
                    values_string += str(value)
            values_string += ", "
        values_string = values_string[:-2]
        values_string += "), "
    values_string = values_string[:-2]
    return values_string

# helper function to parse unaware date into datetime object in UTC
def parse_date(date):
    if date is None or date == "":
        return datetime.datetime.min
    else:
        unaware = datetime.datetime.strptime(date, "%Y-%m-%d")
        tz = pytz.timezone("America/New_York")
        aware = tz.localize(unaware)
        return aware.astimezone(pytz.utc)

# helper function to parse unaware date into datetime object in UTC, different format
def parse_date2(date):
    if date is None or date == "":
        return datetime.datetime.min
    else:
        unaware = datetime.datetime.strptime(date, "%d-%b-%y")
        tz = pytz.timezone("America/New_York")
        aware = tz.localize(unaware)
        return aware.astimezone(pytz.utc)

# main helper function to help with looping
def loop_data_load(section):

    # randomly select loaded contributions table to get around 1000 inserts per day
    loaded_contributions_tables = [
        "loaded_contributions22_1",
        "loaded_contributions22_2",
        "loaded_contributions22_3",
        "loaded_contributions22_4",
        "loaded_contributions22_5",
        "loaded_contributions22_6"
    ]
    loaded_contributions_table = random.choice(loaded_contributions_tables)

    # count total rows inserted
    count = 0

    if section == 0:
        # load candidates
        logger.info(' - '.join(['INFO', 'loading candidates']))

        query_job = client.query("""
        SELECT a.cand_id, a.cand_name, a.cand_pty_affiliation, a.cand_election_yr, a.cand_office_st, a.cand_office, a.cand_office_district, a.cand_ici
        FROM `federal_fec.candidates22` a
        LEFT JOIN `federal_fec.loaded_candidates22` b
        ON a.cand_id = b.cand_id
        WHERE b.cand_id IS NULL AND a.cand_office_district IS NOT NULL
        LIMIT 1000
        """, job_config=gen_job_config())
        df = query_job.result().to_dataframe()
        assert query_job.state == "DONE"

        values = []
        candidates = []
        parties = []
        races = []
        for index, row in df.iterrows():
            candidates.append({
                "cand_id": row["cand_id"],
                "cand_name": row["cand_name"],
                "cand_pty_affiliation": row["cand_pty_affiliation"],
                "cand_election_yr": row["cand_election_yr"],
                "cand_office_st": row["cand_office_st"],
                "cand_office": row["cand_office"],
                "cand_office_district": row["cand_office_district"],
                "cand_ici": row["cand_ici"]
            })
            if row["cand_pty_affiliation"] is not None:
                parties.append({
                    "cand_id": row["cand_id"],
                    "cand_pty_affiliation": row["cand_pty_affiliation"]
                })
            races.append({
                "cand_id": row["cand_id"],
                "cand_election_yr": row["cand_election_yr"],
                "cand_office_st": row["cand_office_st"],
                "cand_office": row["cand_office"],
                "cand_office_district": row["cand_office_district"]
            })
            values.append(row["cand_id"])

        if len(values) > 0:
            with driver.session() as neo4j:
                neo4j.write_transaction(cypher.merge_node_candidate, batch=candidates)
                neo4j.write_transaction(cypher.merge_rel_candidate_party, batch=parties)
                neo4j.write_transaction(cypher.merge_rel_candidate_race, batch=races)
            values_string = get_values_string(values)
            query_job = client.query("""
            INSERT INTO `federal_fec.loaded_candidates22` (cand_id)
            VALUES %s
            """ % (values_string), job_config=gen_job_config())
            query_job.result()
            assert query_job.state == "DONE"
            count += query_job.num_dml_affected_rows

        if count == 0:
            section += 1

        logger.info(' - '.join(['SUCCESS', 'loaded candidates', str(count)]))

    if section == 1:
        # load committees
        logger.info(' - '.join(['INFO', 'loading committees']))

        query_job = client.query("""
        SELECT a.cmte_id, a.cmte_nm, a.cmte_dsgn, a.cmte_tp, a.cmte_pty_affiliation, a.org_tp, a.connected_org_nm
        FROM `federal_fec.committees22` a
        LEFT JOIN `federal_fec.loaded_committees22` b
        ON a.cmte_id = b.cmte_id
        WHERE b.cmte_id IS NULL
        LIMIT 1000
        """, job_config=gen_job_config())
        df = query_job.result().to_dataframe()
        assert query_job.state == "DONE"

        values = []
        committees = []
        parties = []
        employers = []
        for index, row in df.iterrows():
            committees.append({
                "cmte_id": row["cmte_id"],
                "cmte_nm": row["cmte_nm"],
                "cmte_dsgn": row["cmte_dsgn"],
                "cmte_tp": row["cmte_tp"],
                "cmte_pty_affiliation": row["cmte_pty_affiliation"],
                "org_tp": row["org_tp"],
                "connected_org_nm": row["connected_org_nm"]
            })
            if row["cmte_pty_affiliation"] is not None:
                parties.append({
                    "cmte_id": row["cmte_id"],
                    "cmte_pty_affiliation": row["cmte_pty_affiliation"]
                })
            if row["connected_org_nm"] is not None:
                employers.append({
                    "cmte_id": row["cmte_id"],
                    "connected_org_nm": row["connected_org_nm"]
                })
            values.append(row["cmte_id"])

        if len(values) > 0:
            with driver.session() as neo4j:
                neo4j.write_transaction(cypher.merge_node_committee, batch=committees)
                neo4j.write_transaction(cypher.merge_rel_committee_party, batch=parties)
                neo4j.write_transaction(cypher.merge_rel_committee_employer, batch=employers)
            values_string = get_values_string(values)
            query_job = client.query("""
            INSERT INTO `federal_fec.loaded_committees22` (cmte_id)
            VALUES %s
            """ % (values_string), job_config=gen_job_config())
            query_job.result()
            assert query_job.state == "DONE"
            count += query_job.num_dml_affected_rows

        if count == 0:
            section += 1

        logger.info(' - '.join(['SUCCESS', 'loaded committees', str(count)]))

    if section == 2:
        # load linkages
        logger.info(' - '.join(['INFO', 'loading linkages']))

        query_job = client.query("""
        SELECT a.cmte_id, a.cand_id, a.cand_election_yr, a.linkage_id
        FROM `federal_fec.ccl22` a
        LEFT JOIN `federal_fec.loaded_linkages22` b
        ON a.linkage_id = b.linkage_id
        WHERE b.linkage_id IS NULL
        LIMIT 1000
        """, job_config=gen_job_config())
        df = query_job.result().to_dataframe()
        assert query_job.state == "DONE"

        values = []
        rows = []
        for index, row in df.iterrows():
            rows.append({
                "cmte_id": row["cmte_id"],
                "cand_id": row["cand_id"],
                "cand_election_yr": row["cand_election_yr"],
                "linkage_id": row["linkage_id"]
            })
            values.append(row["linkage_id"])

        if len(values) > 0:
            with driver.session() as neo4j:
                neo4j.write_transaction(cypher.merge_rel_committee_associated_with, batch=rows)
            values_string = get_values_string(values)
            query_job = client.query("""
            INSERT INTO `federal_fec.loaded_linkages22` (linkage_id)
            VALUES %s
            """ % (values_string), job_config=gen_job_config())
            query_job.result()
            assert query_job.state == "DONE"
            count += query_job.num_dml_affected_rows

        if count == 0:
            section += 1

        logger.info(' - '.join(['SUCCESS', 'loaded linkages', str(count)]))

    if section == 3:
        # load contributions from committees
        logger.info(' - '.join(['INFO', 'loading contributions from committees']))

        query_job = client.query("""
        SELECT a.source, a.target, a.transaction_dt, a.transaction_amt, a.amndt_ind, a.rpt_tp, a.transaction_pgi, a.transaction_tp, a.image_num, a.file_num, a.tran_id, a.sub_id
        FROM `federal_fec.contributions_from_committees22` a
        LEFT JOIN `federal_fec.loaded_contributions22` b
        ON a.sub_id = b.sub_id
        WHERE b.sub_id IS NULL
        LIMIT 1000
        """, job_config=gen_job_config())
        df = query_job.result().to_dataframe()
        assert query_job.state == "DONE"

        values = []
        rows_with_date = []
        rows_without_date = []
        for index, row in df.iterrows():
            record = {
                "source": row["source"],
                "target": row["target"],
                "transaction_amt": row["transaction_amt"],
                "amndt_ind": row["amndt_ind"],
                "rpt_tp": row["rpt_tp"],
                "transaction_pgi": row["transaction_pgi"],
                "transaction_tp": row["transaction_tp"],
                "image_num": row["image_num"],
                "file_num": row["file_num"],
                "tran_id": row["tran_id"],
                "sub_id": row["sub_id"]
            }
            if row["transaction_dt"] is not None:
                date = parse_date(row["transaction_dt"])
                record.update({
                    "year": date.year,
                    "month": date.month,
                    "day": date.day,
                    "hour": date.hour,
                    "minute": date.minute,
                })
                rows_with_date.append(record)
            else:
                rows_without_date.append(record)
            values.append(row["sub_id"])

        if len(values) > 0:
            with driver.session() as neo4j:
                neo4j.write_transaction(cypher.merge_rel_committee_contributed_to_with_date, batch=rows_with_date)
                neo4j.write_transaction(cypher.merge_rel_committee_contributed_to_without_date, batch=rows_without_date)
            values_string = get_values_string(values)
            query_job = client.query(f"""
            INSERT INTO `federal_fec.{loaded_contributions_table}` (sub_id)
            VALUES %s
            """ % (values_string), job_config=gen_job_config())
            query_job.result()
            assert query_job.state == "DONE"
            count += query_job.num_dml_affected_rows

        if count == 0:
            section += 1

        logger.info(' - '.join(['SUCCESS', 'loaded contributions from committees', str(count)]))

    if section == 4:
        # load contributions from candidates
        logger.info(' - '.join(['INFO', 'loading contributions from candidates']))

        query_job = client.query("""
        SELECT a.source, a.target, a.transaction_dt, a.transaction_amt, a.amndt_ind, a.rpt_tp, a.transaction_pgi, a.transaction_tp, a.image_num, a.file_num, a.tran_id, a.sub_id
        FROM `federal_fec.contributions_from_candidates22` a
        LEFT JOIN `federal_fec.loaded_contributions22` b
        ON a.sub_id = b.sub_id
        WHERE b.sub_id IS NULL
        LIMIT 1000
        """, job_config=gen_job_config())
        df = query_job.result().to_dataframe()
        assert query_job.state == "DONE"

        values = []
        rows_with_date = []
        rows_without_date = []
        for index, row in df.iterrows():
            record = {
                "source": row["source"],
                "target": row["target"],
                "transaction_amt": row["transaction_amt"],
                "amndt_ind": row["amndt_ind"],
                "rpt_tp": row["rpt_tp"],
                "transaction_pgi": row["transaction_pgi"],
                "transaction_tp": row["transaction_tp"],
                "image_num": row["image_num"],
                "file_num": row["file_num"],
                "tran_id": row["tran_id"],
                "sub_id": row["sub_id"]
            }
            if row["transaction_dt"] is not None:
                date = parse_date(row["transaction_dt"])
                record.update({
                    "year": date.year,
                    "month": date.month,
                    "day": date.day,
                    "hour": date.hour,
                    "minute": date.minute,
                })
                rows_with_date.append(record)
            else:
                rows_without_date.append(record)
            values.append(row["sub_id"])

        if len(values) > 0:
            with driver.session() as neo4j:
                neo4j.write_transaction(cypher.merge_rel_candidate_contributed_to_with_date, batch=rows_with_date)
                neo4j.write_transaction(cypher.merge_rel_candidate_contributed_to_without_date, batch=rows_without_date)
            values_string = get_values_string(values)
            query_job = client.query(f"""
            INSERT INTO `federal_fec.{loaded_contributions_table}` (sub_id)
            VALUES %s
            """ % (values_string), job_config=gen_job_config())
            query_job.result()
            assert query_job.state == "DONE"
            count += query_job.num_dml_affected_rows

        if count == 0:
            section += 1

        logger.info(' - '.join(['SUCCESS', 'loaded contributions from candidates', str(count)]))

    if section == 5:
        # load contributions from individual donors
        logger.info(' - '.join(['INFO', 'loading contributions from individual donors']))

        query_job = client.query("""
        SELECT a.entity_tp, a.name, a.state, a.zip_code, a.employer, a.occupation, a.target, a.transaction_dt, a.transaction_amt, a.amndt_ind, a.rpt_tp, a.transaction_pgi, a.transaction_tp, a.image_num, a.file_num, a.tran_id, a.sub_id
        FROM `federal_fec.contributions_from_ind_donors22` a
        LEFT JOIN `federal_fec.loaded_contributions22` b
        ON a.sub_id = b.sub_id
        WHERE b.sub_id IS NULL
        LIMIT 1000
        """, job_config=gen_job_config())
        df = query_job.result().to_dataframe()
        assert query_job.state == "DONE"

        values = []
        rows_with_date = []
        rows_without_date = []
        states = []
        zips = []
        for index, row in df.iterrows():
            record = {
                "entity_tp": row["entity_tp"],
                "name": row["name"].strip() if row["name"] is not None else "",
                "state": row["state"],
                "zip_code": row["zip_code"],
                "employer": row["employer"].strip() if row["employer"] is not None else "",
                "occupation": row["occupation"].strip() if row["occupation"] is not None else "",
                "target": row["target"],
                "transaction_amt": row["transaction_amt"],
                "amndt_ind": row["amndt_ind"],
                "rpt_tp": row["rpt_tp"],
                "transaction_pgi": row["transaction_pgi"],
                "transaction_tp": row["transaction_tp"],
                "image_num": row["image_num"],
                "file_num": row["file_num"],
                "tran_id": row["tran_id"],
                "sub_id": row["sub_id"]
            }
            if row["transaction_dt"] is not None:
                date = parse_date(row["transaction_dt"])
                record.update({
                    "year": date.year,
                    "month": date.month,
                    "day": date.day,
                    "hour": date.hour,
                    "minute": date.minute,
                })
                rows_with_date.append(record)
            else:
                rows_without_date.append(record)
            if row["state"] is not None:
                states.append({
                    "name": row["name"].strip() if row["name"] is not None else "",
                    "zip_code": row["zip_code"],
                    "state": row["state"]
                })
            if row["zip_code"] is not None:
                zips.append({
                    "name": row["name"].strip() if row["name"] is not None else "",
                    "zip_code": row["zip_code"]
                })
            values.append(row["sub_id"])

        if len(values) > 0:
            with driver.session() as neo4j:
                neo4j.write_transaction(cypher.merge_rel_donor_ind_contributed_to_with_date, batch=rows_with_date)
                neo4j.write_transaction(cypher.merge_rel_donor_ind_contributed_to_without_date, batch=rows_without_date)
                neo4j.write_transaction(cypher.merge_rel_donor_ind_state, batch=states)
                neo4j.write_transaction(cypher.merge_rel_donor_ind_zip, batch=zips)
            values_string = get_values_string(values)
            query_job = client.query(f"""
            INSERT INTO `federal_fec.{loaded_contributions_table}` (sub_id)
            VALUES %s
            """ % (values_string), job_config=gen_job_config())
            query_job.result()
            assert query_job.state == "DONE"
            count += query_job.num_dml_affected_rows

        if count == 0:
            section += 1

        logger.info(' - '.join(['SUCCESS', 'loaded contributions from individual donors', str(count)]))

    if section == 6:
        # load contributions from organization donors
        logger.info(' - '.join(['INFO', 'loading contributions from organization donors']))

        query_job = client.query("""
        SELECT a.entity_tp, a.name, a.state, a.zip_code, a.target, a.transaction_dt, a.transaction_amt, a.amndt_ind, a.rpt_tp, a.transaction_pgi, a.transaction_tp, a.image_num, a.file_num, a.tran_id, a.sub_id
        FROM `federal_fec.contributions_from_org_donors22` a
        LEFT JOIN `federal_fec.loaded_contributions22` b
        ON a.sub_id = b.sub_id
        WHERE b.sub_id IS NULL
        LIMIT 1000
        """, job_config=gen_job_config())
        df = query_job.result().to_dataframe()
        assert query_job.state == "DONE"

        values = []
        rows_with_date = []
        rows_without_date = []
        for index, row in df.iterrows():
            record = {
                "entity_tp": row["entity_tp"],
                "name": row["name"].strip() if row["name"] is not None else "",
                "state": row["state"],
                "zip_code": row["zip_code"],
                "target": row["target"],
                "transaction_amt": row["transaction_amt"],
                "amndt_ind": row["amndt_ind"],
                "rpt_tp": row["rpt_tp"],
                "transaction_pgi": row["transaction_pgi"],
                "transaction_tp": row["transaction_tp"],
                "image_num": row["image_num"],
                "file_num": row["file_num"],
                "tran_id": row["tran_id"],
                "sub_id": row["sub_id"]
            }
            if row["transaction_dt"] is not None:
                date = parse_date(row["transaction_dt"])
                record.update({
                    "year": date.year,
                    "month": date.month,
                    "day": date.day,
                    "hour": date.hour,
                    "minute": date.minute,
                })
                rows_with_date.append(record)
            else:
                rows_without_date.append(record)
            values.append(row["sub_id"])

        if len(values) > 0:
            with driver.session() as neo4j:
                neo4j.write_transaction(cypher.merge_rel_donor_org_contributed_to_with_date, batch=rows_with_date)
                neo4j.write_transaction(cypher.merge_rel_donor_org_contributed_to_without_date, batch=rows_without_date)
            values_string = get_values_string(values)
            query_job = client.query(f"""
            INSERT INTO `federal_fec.{loaded_contributions_table}` (sub_id)
            VALUES %s
            """ % (values_string), job_config=gen_job_config())
            query_job.result()
            assert query_job.state == "DONE"
            count += query_job.num_dml_affected_rows

        if count == 0:
            section += 1

        logger.info(' - '.join(['SUCCESS', 'loaded contributions from organization donors', str(count)]))

    if section == 7:
        # load independent expenditures
        logger.info(' - '.join(['INFO', 'loading independent expenditures']))

        query_job = client.query("""
        SELECT a.can_id, a.spe_id, a.exp_amo, a.exp_dat, a.sup_opp, a.pur, a.pay, a.file_num, a.amn_ind, a.tra_id, a.ima_num, a.prev_file_num
        FROM `federal_fec.independent_expenditure_2022` a
        LEFT JOIN `federal_fec.loaded_independent_expenditure_2022` b
        ON a.file_num = b.file_num
        AND a.tra_id = b.tra_id
        WHERE b.file_num IS NULL AND b.tra_id IS NULL
        AND a.can_id != "" AND a.spe_id != ""
        ORDER BY a.ima_num
        LIMIT 1000
        """, job_config=gen_job_config())
        df = query_job.result().to_dataframe()
        df = df.replace({np.nan: None})
        assert query_job.state == "DONE"

        values = []
        new_rows_with_date = []
        new_rows_without_date = []
        amend_rows_with_date = []
        amend_rows_without_date = []
        for index, row in df.iterrows():
            record = {
                "cand_id": row["can_id"],
                "cmte_id": row["spe_id"],
                "exp_amt": row["exp_amo"],
                "sup_opp": row["sup_opp"],
                "purpose": row["pur"].upper().strip() if row["pur"] is not None else "",
                "payee": row["pay"].upper().strip() if row["pay"] is not None else "",
                "amndt_ind": row["amn_ind"],
                "image_num": row["ima_num"],
                "tran_id": row["tra_id"],
                "file_num": row["file_num"],
                "prev_file_num": row["prev_file_num"]
            }
            if row["exp_dat"] is not None:
                date = parse_date2(row["exp_dat"])
                record.update({
                    "year": date.year,
                    "month": date.month,
                    "day": date.day,
                    "hour": date.hour,
                    "minute": date.minute,
                })
                if row["prev_file_num"] is not None:
                    amend_rows_with_date.append(record)
                else:
                    new_rows_with_date.append(record)
            else:
                if row["prev_file_num"] is not None:
                    amend_rows_without_date.append(record)
                else:
                    new_rows_without_date.append(record)
            values.append([row["file_num"], row["tra_id"]])

        if len(values) > 0:
            with driver.session() as neo4j:
                neo4j.write_transaction(cypher.merge_ind_exp_new_with_date, batch=new_rows_with_date)
                neo4j.write_transaction(cypher.merge_ind_exp_new_without_date, batch=new_rows_without_date)
                neo4j.write_transaction(cypher.merge_ind_exp_amend_with_date, batch=amend_rows_with_date)
                neo4j.write_transaction(cypher.merge_ind_exp_amend_without_date, batch=amend_rows_without_date)
            values_string = get_values_string_rows(values)
            query_job = client.query("""
            INSERT INTO `federal_fec.loaded_independent_expenditure_2022` (file_num, tra_id)
            VALUES %s
            """ % (values_string), job_config=gen_job_config())
            query_job.result()
            assert query_job.state == "DONE"
            count += query_job.num_dml_affected_rows

        logger.info(' - '.join(['SUCCESS', 'loaded independent expenditures', str(count)]))

    return {
        "count": count,
        "section": section
    }

# load FEC data into graph
def federal_fec_compute_load_graph(message, context):

    # count total number of rows loaded
    loaded = 0

    # count rows loaded per batch
    count = 0

    # get start time
    start = time.time()

    # loop for 500s
    section = 0
    while time.time()-start < 500:
        result = loop_data_load(section)
        count = result["count"]
        section = result["section"]
        loaded += count
        if count == 0:
            break

    # log progress
    if count == 0:
        logger.info(' - '.join(['FINISHED LOADING ALL FEC DATA INTO GRAPH', str(loaded)]))
    else:
        logger.info(' - '.join(['LOADED BATCH OF FEC DATA INTO GRAPH', str(loaded)]))

    return loaded
