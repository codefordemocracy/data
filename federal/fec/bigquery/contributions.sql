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

CREATE VIEW `federal_fec.contributions_graph22` AS (
  SELECT null AS entity_tp, null AS name, null AS state, null AS zip_code, null AS employer, null AS occupation, source, target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions_from_candidates22`
  UNION ALL
  SELECT entity_tp, name, state, zip_code, employer, occupation, null AS source, target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions_from_ind_donors22`
  UNION ALL
  SELECT entity_tp, name, state, zip_code, null AS employer, null AS occupation, null AS source, target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions_from_org_donors22`
  UNION ALL
  SELECT null AS entity_tp, null AS name, null AS state, null AS zip_code, null AS employer, null AS occupation, source, target, transaction_dt, transaction_amt, amndt_ind, rpt_tp, transaction_pgi, transaction_tp, image_num, file_num, tran_id, sub_id
  FROM `federal_fec.contributions_from_committees22`
);
