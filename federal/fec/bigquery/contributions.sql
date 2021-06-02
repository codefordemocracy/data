-- create views for each type of contribution

CREATE VIEW `federal_fec.contributions_from_candidates22` AS (
  SELECT other_id AS source, cmte_id AS target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions22`
  WHERE entity_tp = "CAN" AND other_id NOT LIKE "C%" AND NOT ((SUBSTR(transaction_tp, 0, 1) = "2" OR SUBSTR(transaction_tp, 0, 1) = "4") AND transaction_tp != "24I" AND transaction_tp != "24T")
  AND other_id is NOT NULL and cmte_id is NOT NULL
);

CREATE VIEW `federal_fec.contributions_from_ind_donors22` AS (
  SELECT entity_tp, name, state, IFNULL(zip_code, '') AS zip_code, employer, occupation, cmte_id AS target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions22`
  WHERE entity_tp = "IND" AND NOT ((SUBSTR(transaction_tp, 0, 1) = "2" OR SUBSTR(transaction_tp, 0, 1) = "4") AND transaction_tp != "24I" AND transaction_tp != "24T")
  AND name is NOT NULL and cmte_id is NOT NULL
);

CREATE VIEW `federal_fec.contributions_from_org_donors22` AS (
  SELECT entity_tp, name, state, IFNULL(zip_code, '') AS zip_code, cmte_id AS target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions22`
  WHERE entity_tp = "ORG" AND other_id IS NULL AND NOT ((SUBSTR(transaction_tp, 0, 1) = "2" OR SUBSTR(transaction_tp, 0, 1) = "4") AND transaction_tp != "24I" AND transaction_tp != "24T")
  AND name is NOT NULL and cmte_id is NOT NULL
);

CREATE VIEW `federal_fec.contributions_com_receipts22` AS (
  -- CCM COM PAC PTY receipts
  SELECT other_id AS source, cmte_id AS target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions22`
  WHERE (entity_tp = "CCM" OR entity_tp = "COM" OR entity_tp = "PAC" OR entity_tp = "PTY") AND NOT ((SUBSTR(transaction_tp, 0, 1) = "2" OR SUBSTR(transaction_tp, 0, 1) = "4") AND transaction_tp != "24I" AND transaction_tp != "24T")
  AND other_id is NOT NULL and cmte_id is NOT NULL
);

CREATE VIEW `federal_fec.contributions_com_disbursements22` AS (
  -- CCM COM PAC PTY disbursements
  SELECT cmte_id AS source, other_id AS target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions22`
  WHERE (entity_tp = "CCM" OR entity_tp = "COM" OR entity_tp = "PAC" OR entity_tp = "PTY") AND ((SUBSTR(transaction_tp, 0, 1) = "2" OR SUBSTR(transaction_tp, 0, 1) = "4") AND transaction_tp != "24I" AND transaction_tp != "24T")
  AND cmte_id is NOT NULL and other_id is NOT NULL
);

CREATE VIEW `federal_fec.contributions_can_disbursements22` AS (
  -- CAN disbursements
  SELECT cmte_id AS source, other_id AS target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions22`
  WHERE entity_tp = "CAN" AND other_id LIKE "C%" AND ((SUBSTR(transaction_tp, 0, 1) = "2" OR SUBSTR(transaction_tp, 0, 1) = "4") AND transaction_tp != "24I" AND transaction_tp != "24T")
  AND cmte_id is NOT NULL and other_id is NOT NULL
);

CREATE VIEW `federal_fec.contributions_org_receipts22` AS (
  -- ORG receipts
  SELECT other_id AS source, cmte_id AS target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions22`
  WHERE entity_tp = "ORG" AND other_id LIKE "C%" AND NOT ((SUBSTR(transaction_tp, 0, 1) = "2" OR SUBSTR(transaction_tp, 0, 1) = "4") AND transaction_tp != "24I" AND transaction_tp != "24T")
  AND other_id is NOT NULL and cmte_id is NOT NULL
);

CREATE VIEW `federal_fec.contributions_org_disbursements22` AS (
  -- ORG disbursements
  SELECT cmte_id AS source, other_id AS target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions22`
  WHERE entity_tp = "ORG" AND other_id LIKE "C%" AND ((SUBSTR(transaction_tp, 0, 1) = "2" OR SUBSTR(transaction_tp, 0, 1) = "4") AND transaction_tp != "24I" AND transaction_tp != "24T")
  AND cmte_id is NOT NULL and other_id is NOT NULL
);

CREATE VIEW `federal_fec.contributions_from_committees22` AS (
  SELECT source, target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions_com_receipts22`
  UNION ALL
  SELECT source, target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions_com_disbursements22`
  UNION ALL
  SELECT source, target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions_can_disbursements22`
  UNION ALL
  SELECT source, target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions_org_receipts22`
  UNION ALL
  SELECT source, target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions_org_disbursements22`
);

CREATE VIEW `federal_fec.contributions_elastic22` AS (
  -- contributions from candidates
  SELECT 'candidate' AS classification, null AS donor_entity_tp, null AS donor_name, null AS donor_state, null AS donor_zip_code, null AS donor_employer, null AS donor_occupation,
    a.source,
      b.cand_name AS source_cand_name, b.cand_pty_affiliation AS source_cand_pty_affiliation, b.cand_election_yr AS source_cand_election_yr, b.cand_office_st AS source_cand_office_st, b.cand_office AS source_cand_office, b.cand_office_district AS source_cand_office_district, b.cand_ici AS source_cand_ici, b.cand_pcc AS source_cand_pcc, b.cand_zip AS source_cand_zip,
      null AS source_cmte_nm, null AS source_cmte_zip, null AS source_cmte_dsgn, null AS source_cmte_tp, null AS source_cmte_pty_affiliation, null AS source_cmte_filing_freq, null AS source_org_tp, null AS source_connected_org_nm,
    a.target, c.cmte_nm AS target_cmte_nm, c.cmte_zip AS target_cmte_zip, c.cmte_dsgn AS target_cmte_dsgn, c.cmte_tp AS target_cmte_tp, c.cmte_pty_affiliation AS target_cmte_pty_affiliation, c.cmte_filing_freq AS target_cmte_filing_freq, c.org_tp AS target_org_tp, c.connected_org_nm AS target_connected_org_nm,
    a.transaction_dt, a.transaction_amt, a.amndt_ind, a.rpt_tp, a.transaction_pgi, a.transaction_tp, a.image_num, a.file_num, a.tran_id, a.sub_id
  FROM `federal_fec.contributions_from_candidates22` a
  LEFT JOIN `federal_fec.cn22` b
    ON a.source = b.cand_id
  LEFT JOIN `federal_fec.cm22` c
    ON a.target = c.cmte_id
  UNION ALL
  -- contributions from individuals
  SELECT 'individual' AS classification, entity_tp AS donor_entity_tp, name AS donor_name, state AS donor_state, zip_code AS donor_zip_code, employer AS donor_employer, occupation AS donor_occupation,
    null AS source,
      null AS source_cand_name, null AS source_cand_pty_affiliation, null AS source_cand_election_yr, null AS source_cand_office_st, null AS source_cand_office, null AS source_cand_office_district, null AS source_cand_ici, null AS source_cand_pcc, null AS source_cand_zip,
      null AS source_cmte_nm, null AS source_cmte_zip, null AS source_cmte_dsgn, null AS source_cmte_tp, null AS source_cmte_pty_affiliation, null AS source_cmte_filing_freq, null AS source_org_tp, null AS source_connected_org_nm,
    a.target, c.cmte_nm AS target_cmte_nm, c.cmte_zip AS target_cmte_zip, c.cmte_dsgn AS target_cmte_dsgn, c.cmte_tp AS target_cmte_tp, c.cmte_pty_affiliation AS target_cmte_pty_affiliation, c.cmte_filing_freq AS target_cmte_filing_freq, c.org_tp AS target_org_tp, c.connected_org_nm AS target_connected_org_nm,
    a.transaction_dt, a.transaction_amt, a.amndt_ind, a.rpt_tp, a.transaction_pgi, a.transaction_tp, a.image_num, a.file_num, a.tran_id, a.sub_id
  FROM `federal_fec.contributions_from_ind_donors22` a
  LEFT JOIN `federal_fec.cm22` c
    ON a.target = c.cmte_id
  UNION ALL
  -- contributions from organizations
  SELECT 'organization' AS classification, entity_tp AS donor_entity_tp, name AS donor_name, state AS donor_state, zip_code AS donor_zip_code, null AS donor_employer, null AS donor_occupation,
    null AS source,
      null AS source_cand_name, null AS source_cand_pty_affiliation, null AS source_cand_election_yr, null AS source_cand_office_st, null AS source_cand_office, null AS source_cand_office_district, null AS source_cand_ici, null AS source_cand_pcc, null AS source_cand_zip,
      null AS source_cmte_nm, null AS source_cmte_zip, null AS source_cmte_dsgn, null AS source_cmte_tp, null AS source_cmte_pty_affiliation, null AS source_cmte_filing_freq, null AS source_org_tp, null AS source_connected_org_nm,
    a.target, c.cmte_nm AS target_cmte_nm, c.cmte_zip AS target_cmte_zip, c.cmte_dsgn AS target_cmte_dsgn, c.cmte_tp AS target_cmte_tp, c.cmte_pty_affiliation AS target_cmte_pty_affiliation, c.cmte_filing_freq AS target_cmte_filing_freq, c.org_tp AS target_org_tp, c.connected_org_nm AS target_connected_org_nm,
    a.transaction_dt, a.transaction_amt, a.amndt_ind, a.rpt_tp, a.transaction_pgi, a.transaction_tp, a.image_num, a.file_num, a.tran_id, a.sub_id
  FROM `federal_fec.contributions_from_org_donors22` a
  LEFT JOIN `federal_fec.cm22` c
    ON a.target = c.cmte_id
  UNION ALL
  -- contributions from committees
  SELECT 'committee' AS classification, null AS donor_entity_tp, null AS donor_name, null AS donor_state, null AS donor_zip_code, null AS donor_employer, null AS donor_occupation,
    a.source,
      null AS source_cand_name, null AS source_cand_pty_affiliation, null AS source_cand_election_yr, null AS source_cand_office_st, null AS source_cand_office, null AS source_cand_office_district, null AS source_cand_ici, null AS source_cand_pcc, null AS source_cand_zip,
      b.cmte_nm AS source_cmte_nm, b.cmte_zip AS source_cmte_zip, b.cmte_dsgn AS source_cmte_dsgn, b.cmte_tp AS source_cmte_tp, b.cmte_pty_affiliation AS source_cmte_pty_affiliation, b.cmte_filing_freq AS source_cmte_filing_freq, b.org_tp AS source_org_tp, b.connected_org_nm AS source_connected_org_nm,
    a.target, c.cmte_nm AS target_cmte_nm, c.cmte_zip AS target_cmte_zip, c.cmte_dsgn AS target_cmte_dsgn, c.cmte_tp AS target_cmte_tp, c.cmte_pty_affiliation AS target_cmte_pty_affiliation, c.cmte_filing_freq AS target_cmte_filing_freq, c.org_tp AS target_org_tp, c.connected_org_nm AS target_connected_org_nm,
    a.transaction_dt, a.transaction_amt, a.amndt_ind, a.rpt_tp, a.transaction_pgi, a.transaction_tp, a.image_num, a.file_num, a.tran_id, a.sub_id
  FROM `federal_fec.contributions_from_committees22` a
  LEFT JOIN `federal_fec.cm22` b
    ON a.source = b.cmte_id
  LEFT JOIN `federal_fec.cm22` c
    ON a.target = c.cmte_id
);
