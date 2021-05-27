import datetime
from dateutil.parser import parse
from irsx.xmlrunner import XMLRunner


def recipient_stub_from_grant(xmlGrant):
    # EIN in xmlGrant is funder, not recipient, so can't be used here
    payload = {
        'name': xmlGrant.get('RcpntPrsnNm', None),
        'businessName1': xmlGrant.get('BsnssNmLn1Txt', None),
        'businessName2': xmlGrant.get('BsnssNmLn2Txt', None),
        'foundationStatus': xmlGrant.get('RcpntFndtnSttsTxt', None),
    }
    if 'AddrssLn1Txt' in xmlGrant.keys() and 'CtyNm' in xmlGrant.keys() and 'SttAbbrvtnCd' in xmlGrant.keys() and 'ZIPCd' in xmlGrant.keys():
        payload['usAddress'] = {
            'address1': xmlGrant['AddrssLn1Txt'],
            'city': xmlGrant['CtyNm'],
            'stateCode': xmlGrant['SttAbbrvtnCd'],
            'zipCode': xmlGrant['ZIPCd'],
        }
        if 'AddrssLn2Txt' in xmlGrant.keys():
            payload['usAddress']['address2'] = xmlGrant['AddrssLn2Txt']
    if 'AddrssLn1Txt' in xmlGrant.keys() and 'CtyNm' in xmlGrant.keys() and 'PrvncOrSttNm' in xmlGrant.keys() and 'CntryCd' in xmlGrant.keys() and 'FrgnPstlCd' in xmlGrant.keys():
        payload['foreignAddress'] = {
            'address1': xmlGrant['AddrssLn1Txt'],
            'city': xmlGrant['CtyNm'],
            'provinceOrState': xmlGrant['PrvncOrSttNm'],
            'countryCode': xmlGrant['CntryCd'],
            'postalCode': xmlGrant['FrgnPstlCd']
        }
        if 'AddrssLn2Txt' in xmlGrant.keys():
            payload['foreignAddress']['address2'] = xmlGrant['AddrssLn2Txt']
    return payload


def format_grant(xmlGrant, status, taxPeriodBeginning, taxPeriodEnding):
    if status != 'approved' and status != 'paid':
        raise Exception('Status must be approved or paid')
    if isinstance(taxPeriodBeginning, datetime.date) == False or isinstance(taxPeriodEnding, datetime.date) == False:
        raise Exception(
            'taxPeriodBeginning and taxPeriodEnding must be datetime.date')
    payload = {
        'awsObjectId': xmlGrant['object_id'],
        'funderEIN': xmlGrant['ein'],
        'recipientStub': recipient_stub_from_grant(xmlGrant),
        'status': status,
        'fromTaxPeriodBeginning': taxPeriodBeginning.isoformat(),
        'fromTaxPeriodEnding': taxPeriodEnding.isoformat(),
    }
    if 'Amt' in xmlGrant.keys():
        payload['amount'] = abs(int(xmlGrant['Amt'].replace(',', '')))
    if 'RcpntFndtnSttsTxt' in xmlGrant.keys():
        payload['recipientType'] = xmlGrant['RcpntFndtnSttsTxt']
    if 'GrntOrCntrbtnPrpsTxt' in xmlGrant.keys():
        payload['purpose'] = xmlGrant['GrntOrCntrbtnPrpsTxt']
    if 'RcpntRltnshpTxt' in xmlGrant.keys():
        payload['relationship'] = xmlGrant['RcpntRltnshpTxt']
    return payload


def grants_from_990pf(filing):
    pfs = filing.get_parsed_sked('IRS990PF')
    headers = filing.get_parsed_sked('ReturnHeader990x')
    if len(pfs) == 0:
        raise Exception('no form found')
    if len(headers) == 0:
        raise Exception('no header found')
    form990pf = pfs[0]
    header = headers[0]
    header_x = header['schedule_parts'].get('skedd_part_x', None)
    if len(pfs) == 0:
        raise Exception('no form found')
    grants_paid = form990pf['groups'].get('PFGrntOrCntrbtnPdDrYr', [])
    grants_approved = form990pf['groups'].get('PFGrntOrCntrApprvFrFt', [])
    grants = []
    for grant in grants_paid:
        grants.append(format_grant(grant, 'paid', parse(
            header_x['TxPrdBgnDt']), parse(header_x['TxPrdEndDt'])))
    for grant in grants_approved:
        grants.append(format_grant(grant, 'approved', parse(
            header_x['TxPrdBgnDt']), parse(header_x['TxPrdEndDt'])))
    return grants


def related_entity_from_od(officer, relationship):
    personName = None
    if isinstance(officer.get('PersonNm', None), str):
        personName = officer['PersonNm']
    elif 'PersonNm' in officer and '#text' in officer['PersonNm']:
        personName = officer['PersonNm']['#text']

    payload = {
        'relationship': relationship,
        'name1': personName,
        'title': officer.get('TitleTxt', None),
        'totalCompensation': int(officer.get('CompensationAmt', 0)),
        'benefitContributions': int(officer.get('EmployeeBenefitProgramAmt', 0)),
        'expenseAccount': int(officer.get('ExpenseAccountOtherAllwncAmt', 0)),
        'hoursPerWeek': officer.get('AverageHrsPerWkDevotedToPosRt', 0)
    }
    if 'BusinessName' in officer:
        payload['businessName1'] = officer['BusinessName'].get(
            'BusinessNameLine1Txt', None)
        payload['businessName2'] = officer['BusinessName'].get(
            'BusinessNameLine2Txt', None)
    else:
        payload['businessName1'] = officer.get('BusinessNameLine1Txt', None),
        payload['businessName2'] = officer.get('BusinessNameLine2Txt', None),

    if personName == None and payload['businessName1'] != None:
        payload['classification'] = 'business'
    else:
        payload['classification'] = 'person'

    if 'USAddress' in officer and 'AddressLine1Txt' in officer['USAddress']:
        payload['usAddress'] = {
            'address1': officer['USAddress']['AddressLine1Txt'],
            'address2': officer['USAddress'].get('AddressLine2Txt', None),
            'city': officer['USAddress']['CityNm'],
            'stateCode': officer['USAddress']['StateAbbreviationCd'],
            'zipCode': officer['USAddress']['ZIPCd'],
        }
    if 'ForeignAddress' in officer and 'CntryCd' in officer['ForeignAddress']:
        payload['foreignAddress'] = {
            'address1': officer['ForeignAddress']['AddressLine1Txt'],
            'address2': officer['ForeignAddress'].get('AddressLine2Txt', None),
            'city': officer['ForeignAddress']['CityNm'],
            'provinceOrState': officer['ForeignAddress']['ProvinceOrStateNm'],
            'postalCode': officer['ForeignAddress']['ForeignPostalCd'],
            'countryCode': officer['ForeignAddress']['CountryCd'],
        }
    return payload


def org_from_990pf(filing):
    pfs = filing.get_parsed_sked('IRS990PF')
    headers = filing.get_parsed_sked('ReturnHeader990x')
    if len(pfs) == 0:
        raise Exception('no form found')
    if len(headers) == 0:
        raise Exception('no header found')
    doc = pfs[0]
    header = headers[0]
    header_x = header['schedule_parts'].get('skedd_part_x', None)
    if header_x == None:
        return {}
    doc_1 = doc['schedule_parts']['pf_part_i']
    doc_2 = doc['schedule_parts']['pf_part_ii']
    doc_7a = doc['schedule_parts']['pf_part_viia']
    doc_8 = filing.raw_irs_dict['Return']['ReturnData']['IRS990PF']['OfficerDirTrstKeyEmplInfoGrp']
    payload = {
        'ein': header_x['ein'],
        'name1': header_x.get('BsnssNmLn1Txt', None),
        'name2': header_x.get('BsnssNmLn2Txt', None),
        'phone': header_x.get('PhnNm', None),
        'filingType': '990PF',
        'lastFilingAt': parse(header_x['RtrnTs']).isoformat(),
        'lastTaxPeriodBegan': parse(header_x['TxPrdBgnDt']).isoformat(),
        'lastTaxPeriodEnded': parse(header_x['TxPrdEndDt']).isoformat(),
        'website': doc_7a.get('WbstAddrssTxt', None),
    }

    if 'AddrssLn1Txt' in header_x.keys() and 'CtyNm' in header_x.keys() and 'SttAbbrvtnCd' in header_x.keys() and 'ZIPCd'in header_x.keys():
        payload['usAddress'] = {
            'address1': header_x['AddrssLn1Txt'],
            'address2': header_x.get('AddrssLn2Txt', None),
            'city': header_x['CtyNm'],
            'stateCode': header_x['SttAbbrvtnCd'],
            'zipCode': header_x['ZIPCd'],
        }
    if 'CntryCd' in header_x.keys():
        payload['foreignAddress'] = {
            'address1': header_x.get('AddrssLn1Txt', None),
            'address2': header_x.get('AddrssLn2Txt', None),
            'city': header_x.get('CtyNm', None),
            'provinceOrState': header_x.get('PrvncOrSttNm', None),
            'countryCode': header_x['CntryCd'],
            'postalCode': header_x.get('FrgnPstlCd', None)
        }

    payload['financialSummary'] = {
        'contributionsReceived': int(doc_1.get('CntrRcvdRvAndExpnssAmt', 0)),
        'totalRevenue': int(doc_1.get('TtlRvAndExpnssAmt', 0)),
        'totalExpenses': int(doc_1.get('TtlExpnssRvAndExpnssAmt', 0)),
        'contributionsPaid': int(doc_1.get('CntrPdRvAndExpnssAmt', 0)),
        'charitableDistributions': int(doc_1.get('CntrPdDsbrsChrtblAmt', 0)),
        'totalAssets': int(doc_2.get('TtlAsstsEOYFMVAmt', doc_2.get('TtlAsstsEOYAmt', 0))),
        'totalLiabilities': int(doc_2.get('TtlLbltsEOYAmt', 0))
    }
    payload['human_resources'] = {
        'otherEmployeesOver50KCount': int(doc_8.get('OtherEmployeePaidOver50kCnt', 0)),
        'contractorsOver50KCount': int(doc_8.get('ContractorPaidOver50kCnt', 0)),
    }
    payload['relatedEntities'] = []
    if doc_8.get('OfficerDirTrstKeyEmplGrp', None) != None:
        if isinstance(doc_8['OfficerDirTrstKeyEmplGrp'], list):
            for officer in doc_8['OfficerDirTrstKeyEmplGrp']:
                payload['relatedEntities'].append(
                    related_entity_from_od(officer, 'officer/director/manager'))
        else:
            payload['relatedEntities'].append(related_entity_from_od(
                doc_8['OfficerDirTrstKeyEmplGrp'], 'officer/director/manager'))
    if doc_8.get('CompOfHghstPdEmplOrNONETxt', 'NONE') != 'NONE':
        for officer in doc_8['CompOfHghstPdEmplOrNONETxt']:
            payload['relatedEntities'].append(
                related_entity_from_od(officer, 'highly comped employee'))
    if doc_8.get('CompOfHghstPdCntrctOrNONETxt', 'NONE') != 'NONE':
        for officer in doc_8['CompOfHghstPdEmplOrNONETxt']:
            payload['relatedEntities'].append(
                related_entity_from_od(officer, 'highly comped contractor'))
    return payload


def timestamp_now():
    return datetime.datetime.utcnow().isoformat() + 'Z'
