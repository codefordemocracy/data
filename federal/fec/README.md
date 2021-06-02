## Data Sources

We process 13 FEC datasets from their bulk downloads, all of which are stored in separate tables in the *federal* schema in BigQuery.

**Dataset:** All candidates
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/weball20.zip
**Filename:** weball20.txt
**Description:** https://www.fec.gov/campaign-finance-data/all-candidates-file-description/
**Summary:** The all candidate summary file contains one record including summary financial information for all candidates who raised or spent money during the period no matter when they are up for election.

**Dataset:** Candidate master
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/cn20.zip
**Filename:** cn.txt
**Description:** https://www.fec.gov/campaign-finance-data/candidate-master-file-description/
**Summary:** The candidate master file contains one record for each candidate who has either registered with the Federal Election Commission or appeared on a ballot list prepared by a state elections office.

**Dataset:** Candidate-committee linkages
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/ccl20.zip
**Filename:** ccl.txt
**Description:** https://www.fec.gov/campaign-finance-data/candidate-committee-linkage-file-description/
**Summary:** This file contains one record for each candidate to committee linkage.

**Dataset:** House/Senate current campaigns
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/webl20.zip
**Filename:** webl20.txt
**Description:** https://www.fec.gov/campaign-finance-data/current-campaigns-house-and-senate-file-description/
**Summary:** These files contain one record for each campaign. This record contains summary financial information.

**Dataset:** Committee master
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/cm20.zip
**Filename:** cm.txt
**Description:** https://www.fec.gov/campaign-finance-data/committee-master-file-description/
**Summary:** The committee master file contains one record for each committee registered with the Federal Election Commission. This includes federal political action committees and party committees, campaign committees for presidential, house and senate candidates, as well as groups or organizations who are spending money for or against candidates for federal office.

**Dataset:** PAC summary
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/webk20.zip
**Filename:** webk20.txt
**Description:** https://www.fec.gov/campaign-finance-data/pac-and-party-summary-file-description/
**Summary:** This file gives overall receipts and disbursements for each PAC and party committee registered with the commission, along with a breakdown of overall receipts by source and totals for contributions to other committees, independent expenditures made and other information.

**Dataset:** Contributions by individuals
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/indiv20.zip
**Filename:** itcont.txt
**Description:** https://www.fec.gov/campaign-finance-data/contributions-individuals-file-description/
**Summary:** The individual contributions file contains each contribution from an individual to a federal committee. It includes the ID number of the committee receiving the contribution, the name, city, state, zip code, and place of business of the contributor along with the date and amount of the contribution.

**Dataset:** Contributions from committees to candidates & independent expenditures
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/pas220.zip
**Filename:** itpas2.txt
**Description:** https://www.fec.gov/campaign-finance-data/contributions-committees-candidates-file-description/
**Summary:** The itemized committee contributions file contains each contribution or independent expenditure made by a PAC, party committee, candidate committee, or other federal committee to a candidate during the two-year election cycle.

**Dataset:** Any transaction from one committee to another
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/oth20.zip
**Filename:** itoth.txt
**Description:** https://www.fec.gov/campaign-finance-data/any-transaction-one-committee-another-file-description/
**Summary:** The itemized records (miscellaneous transactions) file contains all transactions (contributions, transfers, etc. among federal committees). It contains all data in the itemized committee contributions file plus PAC contributions to party committees, party transfers from state committee to state committee, and party transfers from national committee to state committee. This file only includes federal transfers not soft money transactions.

**Dataset:** Operating expenditures
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/oppexp20.zip
**Filename:** oppexp.txt
**Description:** https://www.fec.gov/campaign-finance-data/operating-expenditures-file-description/
**Summary:** This file contains disbursements reported on FEC Form 3 Line 17, FEC Form 3P Line 23and FEC Form 3X Lines 21(a)(i), 21(a)(ii) and 21(b).

**Dataset:** Independent expenditures
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/independent_expenditure_2020.csv
**Filename:** independent_expenditure_2020.csv
**Description:** https://www.fec.gov/campaign-finance-data/independent-expenditures-file-description/
**Summary:** This file contains "24-hour" and "48-hour" reports of independent expenditures filed during the current election cycle and for election cycles through 2010. The file contains detailed information about independent expenditures, including who was paid, the purpose of the disbursement, date and amount of the expenditure and the candidate for or against whom the expenditure was made.

**Dataset:** Electioneering communications
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/ElectioneeringComm_2020.csv
**Filename:** ElectioneeringComm_2020.csv
**Description:** https://www.fec.gov/campaign-finance-data/electioneering-communications-file-description/
**Summary:** This file contains specific disbursement transactions disclosed to the Commission as electioneering communications.

**Dataset:** Communication costs
**Download Link:** https://www.fec.gov/files/bulk-downloads/2020/CommunicationCosts_2020.csv
**Filename:** CommunicationCosts_2020.csv
**Description:** https://www.fec.gov/campaign-finance-data/communication-costs-file-description
**Summary:** This file contains specific disbursement transactions disclosed to the Commission as communication costs by corporations and labor organizations.

## BigQuery

The *bigquery* folder contains setup scripts for important views that are referenced in the functions.
