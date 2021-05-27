import datetime
from dateutil.parser import parse
from irsx.xmlrunner import XMLRunner


def address_990(data):
    payload = {}
    if 'AddrssLn1Txt' in data.keys() and 'CtyNm' in data.keys() and 'SttAbbrvtnCd' in data.keys() and 'ZIPCd'in data.keys():
        payload['usAddress'] = {
            'address1': data['AddrssLn1Txt'],
            'address2': data.get('AddrssLn2Txt', None),
            'city': data['CtyNm'],
            'stateCode': data['SttAbbrvtnCd'],
            'zipCode': data['ZIPCd'],
        }
    if 'CntryCd' in data.keys():
        payload['foreignAddress'] = {
            'address1': data.get('AddrssLn1Txt', None),
            'address2': data.get('AddrssLn2Txt', None),
            'city': data.get('CtyNm', None),
            'provinceOrState': data.get('PrvncOrSttNm', None),
            'countryCode': data['CntryCd'],
            'postalCode': data.get('FrgnPstlCd', None)
        }
    return payload


def program_accomplishments_990(filing):
    form = filing.get_parsed_sked('IRS990')
    payload = []
    if 'PrgSrvcAccmActyOthr' in form[0]['groups']:
        for section in form[0]['groups']['PrgSrvcAccmActyOthr']:
            if 'Dsc' in section:
                payload.append({
                    'description': section['Dsc'],
                    'grants': int(section.get('GrntAmt', 0)),
                    'revenue': int(section.get('RvnAmt', 0)),
                    'expenses': int(section.get('ExpnsAmt', 0)),
                })
    if 'part_iii' in form[0]['schedule_parts']:
        section = form[0]['schedule_parts']['part_iii']
        payload.append({
            'description': section['Dsc'],
            'grants': int(section.get('GrntAmt', 0)),
            'revenue': int(section.get('RvnAmt', 0)),
            'expenses': int(section.get('ExpnsAmt', 0)),
        })
    return payload


def related_entity_990(section):
    payload = {
        'name1': section.get('PrsnNm', None),
        'businessName1': section.get('BsnssNmLn1Txt', None),
        'businessName2': section.get('BsnssNmLn2Txt', None),
        'title': section.get('TtlTxt', None),
        'totalCompensation': int(section.get('RprtblCmpFrmOrgAmt', 0)),
        'totalCompensationRelated': int(section.get('RprtblCmpFrmRltdOrgAmt', 0)),
        'otherCompensation': int(section.get('OthrCmpnstnAmt', 0)),
        'hoursPerWeek': float(section.get('AvrgHrsPrWkRt', 0)),
        'hoursPerWeekRelated': float(section.get('AvrgHrsPrWkRltdOrgRt', 0)),
    }
    if section.get('IndvdlTrstOrDrctrInd', 'nope').upper() == 'X':
        payload['relationship'] = 'individual trustee/director'
    elif section.get('InstttnlTrstInd', 'nope').upper() == 'X':
        payload['relationship'] = 'institutional trustee'
    elif section.get('OffcrInd', 'nope').upper() == 'X':
        payload['relationship'] = 'officer'
    elif section.get('KyEmplyInd', 'nope').upper() == 'X':
        payload['relationship'] = 'key employee'
    elif section.get('HghstCmpnstdEmplyInd', 'nope').upper() == 'X':
        payload['relationship'] = 'highly comped employee'
    elif section.get('FrmrOfcrDrctrTrstInd', 'nope').upper() == 'X':
        payload['relationship'] = 'former officer/director/manager'

    return payload


def contractor_990(section):
    payload = {
        'relationship': 'highly comped contractor',
        'name1': section.get('PrsnNm', None),
        'businessName1': section.get('BsnssNmLn1Txt', None),
        'businessName2': section.get('BsnssNmLn2Txt', None),
        'totalCompensation': int(section.get('CmpnstnAmt', 0)),
    }
    payload.update(address_990(section))
    return payload


def related_entities_990(filing):
    form = filing.get_parsed_sked('IRS990')
    payload = []
    if 'Frm990PrtVIISctnA' in form[0]['groups']:
        for section in form[0]['groups']['Frm990PrtVIISctnA']:
            payload.append(related_entity_990(section))
    if 'CntrctrCmpnstn' in form[0]['groups']:
        for section in form[0]['groups']['CntrctrCmpnstn']:
            payload.append(contractor_990(section))
    return payload


def financial_summary_990(filing):
    form = filing.get_parsed_sked('IRS990')[0]['schedule_parts']['part_i']
    return {
        'contributionsReceived': int(form.get('CYCntrbtnsGrntsAmt', 0)),
        'programServiceRevenue': int(form.get('CYPrgrmSrvcRvnAmt', 0)),
        'totalRevenue': int(form.get('CYTtlRvnAmt', 0)),
        'totalExpenses': int(form.get('CYTtlExpnssAmt', 0)),
        'totalAssets': int(form.get('TtlAsstsEOYAmt', 0)),
        'totalLiabilities': int(form.get('TtlLbltsEOYAmt', 0)),
        'contributionsPaid': int(form.get('CYGrntsAndSmlrPdAmt', 0)),
    }


def human_resources_990(filing):
    form = filing.get_parsed_sked('IRS990')[0]['schedule_parts']['part_i']
    return {
        'employeeCount': int(form.get('TtlEmplyCnt', 0)),
        'volunteerCount': int(form.get('TtlVlntrsCnt', 0)),
        'votingMembersCount': int(form.get('VtngMmbrsGvrnngBdyCnt', 0)),
        'independentVotingMembersCount': int(form.get('VtngMmbrsIndpndntCnt', 0)),
    }


def basic_org_from_990(filing):
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
        'filingType': '990',
        'lastFilingAt': parse(header_x['RtrnTs']).isoformat(),
        'lastTaxPeriodBegan': parse(header_x['TxPrdBgnDt']).isoformat(),
        'lastTaxPeriodEnded': parse(header_x['TxPrdEndDt']).isoformat(),
    }

    if len(filing.get_parsed_sked('IRS990')) > 0:
        part_0 = filing.get_parsed_sked(
            'IRS990')[0]['schedule_parts'].get('part_0', None)
        part_iii = filing.get_parsed_sked(
            'IRS990')[0]['schedule_parts'].get('part_iii', None)

    if part_iii != None:
        payload['activityOrMission'] = part_iii.get('MssnDsc', None)

    if part_0 != None:
        payload.update(address_990(part_0))

    return payload


def org_from_990(filing):
    org = basic_org_from_990(filing)
    org['financialSummary'] = financial_summary_990(filing)
    org['accomplishments'] = program_accomplishments_990(filing)
    org['relatedEntities'] = related_entities_990(filing)
    org['humanResources'] = human_resources_990(filing)
    return org

def format_grant_990(grant, taxPeriodBeginning, taxPeriodEnding):
    if isinstance(taxPeriodBeginning, datetime.date) == False or isinstance(taxPeriodEnding, datetime.date) == False:
        raise Exception('taxPeriodBeginning and taxPeriodEnding must be datetime.date')
    payload = {
        'amount': int(grant.get('CshGrntAmt', 0)),
        'nonCashAmount': int(grant.get('NnCshAssstncAmt', 0)),
        'funderEIN': grant['ein'],
        'awsObjectId': grant['object_id'],
        'status': 'paid',
        'purpose': grant.get('PrpsOfGrntTxt', grant.get('GrntTxt', grant.get('OfAssstncTxt', None))),
        'fromTaxPeriodBeginning': taxPeriodBeginning,
        'fromTaxPeriodEnding': taxPeriodEnding,
    }
    if 'RgnTxt' in grant:
        payload['region'] = grant.get('RgnTxt', None)
    elif 'RcpntCnt' in grant:
        payload['recipientCount'] = int(grant['RcpntCnt'])
    if 'RcpntEIN' in grant or 'RcpntPrsnNm' in grant or 'BsnssNmLn1Txt' in grant or 'BsnssNmLn2Txt' in grant:
        payload['recipientStub'] = {
            'ein': grant.get('RcpntEIN', None),
            'name': grant.get('RcpntPrsnNm', None),
            'businessName1': grant.get('BsnssNmLn1Txt', None),
            'businessName2': grant.get('BsnssNmLn2Txt', None),
            'foundationStatus': grant.get('RcpntFndtnSttsTxt', None),
        }
        payload['recipientStub'].update(address_990(grant))
    return payload

def grants_from_990(filing):
    headers = filing.get_parsed_sked('ReturnHeader990x')
    if len(headers) == 0:
        raise Exception('no header found')
    header_x = headers[0]['schedule_parts'].get('skedd_part_x', None)
    scheduleIs = filing.get_parsed_sked('IRS990ScheduleI')
    scheduleFs = filing.get_parsed_sked('IRS990ScheduleF')
    
    grants = []
    if len(scheduleIs) > 0:
        if 'SkdIRcpntTbl' in scheduleIs[0]['groups']:
            for xmlGrant in scheduleIs[0]['groups']['SkdIRcpntTbl']:
                grants.append(format_grant_990(xmlGrant, parse(header_x['TxPrdBgnDt']), parse(header_x['TxPrdEndDt'])))
        if 'SkdIGrntsOthrAsstTIndvInUS' in scheduleIs[0]['groups']:
            for xmlGrant in scheduleIs[0]['groups']['SkdIGrntsOthrAsstTIndvInUS']:
                grants.append(format_grant_990(xmlGrant, parse(header_x['TxPrdBgnDt']), parse(header_x['TxPrdEndDt'])))
    
    if len(scheduleFs) > 0:
        if 'SkdFGrntsTOrgOtsdUS' in scheduleFs[0]['groups']:
            for xmlGrant in scheduleFs[0]['groups']['SkdFGrntsTOrgOtsdUS']:
                grants.append(format_grant_990(xmlGrant, parse(header_x['TxPrdBgnDt']), parse(header_x['TxPrdEndDt'])))
        if 'SkdFFrgnIndvdlsGrnts' in scheduleFs[0]['groups']:
            for xmlGrant in scheduleFs[0]['groups']['SkdFFrgnIndvdlsGrnts']:
                grants.append(format_grant_990(xmlGrant, parse(header_x['TxPrdBgnDt']), parse(header_x['TxPrdEndDt'])))
        
    
    return grants