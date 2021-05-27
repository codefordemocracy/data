import datetime
from dateutil.parser import parse
from irsx.xmlrunner import XMLRunner


def financial_summary_990ez(filing):
    ez = filing.get_parsed_sked('IRS990EZ')
    part1 = ez[0]['schedule_parts'].get('ez_part_i', None)
    part2 = ez[0]['schedule_parts']['ez_part_ii']
    payload = {
        'netAssets': int(part2.get('NtAsstsOrFndBlncsEOYAmt', 0)),
    }
    if part1 != None:
        payload['contributionsReceived'] = int(part1.get('CntrbtnsGftsGrntsEtcAmt', 0))
        payload['totalRevenue'] = int(part1.get('TtlRvnAmt', 0))
        payload['totalExpenses'] = int(part1.get('TtlExpnssAmt', 0))
    return payload


def program_accomplishments_990ez(filing):
    ez = filing.get_parsed_sked('IRS990EZ')
    payload = []
    if 'EZPrgrmSrvcAccmplshmnt' in ez[0]['groups']:
        for section in ez[0]['groups']['EZPrgrmSrvcAccmplshmnt']:
            if 'DscrptnPrgrmSrvcAccmTxt' in section:
                payload.append({
                    'description': section['DscrptnPrgrmSrvcAccmTxt'],
                    'grants': int(section.get('GrntsAndAllctnsAmt', 0)),
                    'expenses': int(section.get('PrgrmSrvcExpnssAmt', 0)),
                    'includesForeignGrants': 'FrgnGrntsInd' in section and section['FrgnGrntsInd'].upper() == 'X'
                })
    return payload


def related_entity_990ez(section, relationship):
    return {
        'relationship': relationship,
        'name1': section.get('PrsnNm', None),
        'title': section.get('TtlTxt', None),
        'totalCompensation': int(section.get('CmpnstnAmt', 0)),
        'benefitContributions': int(section.get('EmplyBnftPrgrmAmt', 0)),
        'expenseAccount': int(section.get('ExpnsAccntOthrAllwncAmt', 0)),
        'hoursPerWeek': float(section.get('AvrgHrsPrWkDvtdTPsRt', 0))
    }


def related_entities_990ez(filing):
    ez = filing.get_parsed_sked('IRS990EZ')
    payload = []
    if 'EZOffcrDrctrTrstEmpl' in ez[0]['groups']:
        for section in ez[0]['groups']['EZOffcrDrctrTrstEmpl']:
            payload.append(related_entity_990ez(
                section, 'officer/director/manager'))
    if 'EZCmpnstnHghstPdEmpl' in ez[0]['groups']:
        for section in ez[0]['groups']['EZCmpnstnHghstPdEmpl']:
            payload.append(related_entity_990ez(
                section, 'highly comped employee'))
    return payload


def basic_org_from_990ez(filing):
    headers = filing.get_parsed_sked('ReturnHeader990x')
    if len(headers) == 0:
        raise Exception('no header found')
    header = headers[0]
    header_x = header['schedule_parts'].get('skedd_part_x', None)

    if header_x == None:
        return {}
    payload = {
        'ein': header_x['ein'],
        'name1': header_x.get('BsnssNmLn1Txt', None),
        'name2': header_x.get('BsnssNmLn2Txt', None),
        'phone': header_x.get('PhnNm', None),
        'filingType': '990EZ',
        'lastFilingAt': parse(header_x['RtrnTs']).isoformat(),
        'lastTaxPeriodBegan': parse(header_x['TxPrdBgnDt']).isoformat(),
        'lastTaxPeriodEnded': parse(header_x['TxPrdEndDt']).isoformat(),
    }

    if len(filing.get_parsed_sked('IRS990EZ')) > 0:
        part_0 = filing.get_parsed_sked(
            'IRS990EZ')[0]['schedule_parts'].get('ez_part_0', None)
        part_iii = filing.get_parsed_sked(
            'IRS990EZ')[0]['schedule_parts'].get('ez_part_iii', None)

    if part_0 != None:
        payload['website'] = part_0.get('WbstAddrssTxt', None)

    if part_iii != None:
        payload['activityOrMission'] = part_iii.get('PrmryExmptPrpsTxt', None)

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
    return payload


def org_from_990ez(filing):
    org = basic_org_from_990ez(filing)
    org['financialSummary'] = financial_summary_990ez(filing)
    org['accomplishments'] = program_accomplishments_990ez(filing)
    org['relatedEntities'] = related_entities_990ez(filing)
    return org
