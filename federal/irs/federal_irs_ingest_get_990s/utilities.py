import datetime
import pytz

def get_xml_parts(schedules):

    xml = {}
    for schedule in schedules:

        schedule_name = schedule.pop('schedule_name')

        if schedule_name == 'ReturnHeader990x':

            xml[schedule_name] = {
                'schedule_parts': {}
            }

            # filing info
            if 'returnheader990x_part_i' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['returnheader990x_part_i'] = schedule['schedule_parts']['returnheader990x_part_i']

        elif schedule_name == 'IRS990':

            xml[schedule_name] = {
                'groups': {},
                'schedule_parts': {}
            }

            # org summary
            if 'part_0' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['part_0'] = schedule['schedule_parts']['part_0']
            if 'part_i' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['part_i'] = schedule['schedule_parts']['part_i']

            # activities
            if 'part_iii' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['part_iii'] = schedule['schedule_parts']['part_iii']
            if 'PrgSrvcAccmActyOthr' in schedule['groups']:
                xml[schedule_name]['groups']['PrgSrvcAccmActyOthr'] = schedule['groups']['PrgSrvcAccmActyOthr']

            # people
            if 'Frm990PrtVIISctnA' in schedule['groups']:
                xml[schedule_name]['groups']['Frm990PrtVIISctnA'] = schedule['groups']['Frm990PrtVIISctnA']
            if 'CntrctrCmpnstn' in schedule['groups']:
                xml[schedule_name]['groups']['CntrctrCmpnstn'] = schedule['groups']['CntrctrCmpnstn']

        elif schedule_name == 'IRS990ScheduleC':

            xml[schedule_name] = {
                'groups': {},
                'schedule_parts': {}
            }

            # political expenditures
            if 'skedc_part_0' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['skedc_part_0'] = schedule['schedule_parts']['skedc_part_0']
            if 'skedc_part_iia' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['skedc_part_iia'] = schedule['schedule_parts']['skedc_part_iia']
            if 'skedc_part_iib' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['skedc_part_iib'] = schedule['schedule_parts']['skedc_part_iib']

            # political contributions
            if 'SkdCSctn527PltclOrg' in schedule['groups']:
                xml[schedule_name]['groups']['SkdCSctn527PltclOrg'] = schedule['groups']['SkdCSctn527PltclOrg']

            # description of activities
            if 'SkdCSpplmntlInfrmtnDtl' in schedule['groups']:
                xml[schedule_name]['groups']['SkdCSpplmntlInfrmtnDtl'] = schedule['groups']['SkdCSpplmntlInfrmtnDtl']

        elif schedule_name == 'IRS990ScheduleF':

            xml[schedule_name] = {
                'groups': {}
            }

            # grants outside US
            if 'SkdFGrntsTOrgOtsdUS' in schedule['groups']:
                xml[schedule_name]['groups']['SkdFGrntsTOrgOtsdUS'] = schedule['groups']['SkdFGrntsTOrgOtsdUS']
            if 'SkdFFrgnIndvdlsGrnts' in schedule['groups']:
                xml[schedule_name]['groups']['SkdFFrgnIndvdlsGrnts'] = schedule['groups']['SkdFFrgnIndvdlsGrnts']

        elif schedule_name == 'IRS990ScheduleI':

            xml[schedule_name] = {
                'groups': {}
            }

            # grants
            if 'SkdIRcpntTbl' in schedule['groups']:
                xml[schedule_name]['groups']['SkdIRcpntTbl'] = schedule['groups']['SkdIRcpntTbl']
            if 'SkdIGrntsOthrAsstTIndvInUS' in schedule['groups']:
                xml[schedule_name]['groups']['SkdIGrntsOthrAsstTIndvInUS'] = schedule['groups']['SkdIGrntsOthrAsstTIndvInUS']

        elif schedule_name == 'IRS990ScheduleR':

            xml[schedule_name] = {
                'groups': {}
            }

            # related entities
            if 'SkdRIdDsrgrddEntts' in schedule['groups']:
                xml[schedule_name]['groups']['SkdRIdDsrgrddEntts'] = schedule['groups']['SkdRIdDsrgrddEntts']
            if 'SkdRIdRltdTxExmptOrg' in schedule['groups']:
                xml[schedule_name]['groups']['SkdRIdRltdTxExmptOrg'] = schedule['groups']['SkdRIdRltdTxExmptOrg']
            if 'SkdRIdRltdOrgTxblPrtnrshp' in schedule['groups']:
                xml[schedule_name]['groups']['SkdRIdRltdOrgTxblPrtnrshp'] = schedule['groups']['SkdRIdRltdOrgTxblPrtnrshp']
            if 'SkdRIdRltdOrgTxblCrpTr' in schedule['groups']:
                xml[schedule_name]['groups']['SkdRIdRltdOrgTxblCrpTr'] = schedule['groups']['SkdRIdRltdOrgTxblCrpTr']
            if 'SkdRTrnsctnsRltdOrg' in schedule['groups']:
                xml[schedule_name]['groups']['SkdRTrnsctnsRltdOrg'] = schedule['groups']['SkdRTrnsctnsRltdOrg']
            if 'SkdRUnrltdOrgTxblPrtnrshp' in schedule['groups']:
                xml[schedule_name]['groups']['SkdRUnrltdOrgTxblPrtnrshp'] = schedule['groups']['SkdRUnrltdOrgTxblPrtnrshp']

        elif schedule_name == 'IRS990EZ':

            xml[schedule_name] = {
                'groups': {},
                'schedule_parts': {}
            }

            # org summary
            if 'ez_part_0' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['ez_part_0'] = schedule['schedule_parts']['ez_part_0']
            if 'ez_part_i' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['ez_part_i'] = schedule['schedule_parts']['ez_part_i']

            # activities
            if 'ez_part_iii' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['ez_part_iii'] = schedule['schedule_parts']['ez_part_iii']
            if 'EZPrgrmSrvcAccmplshmnt' in schedule['groups']:
                xml[schedule_name]['groups']['EZPrgrmSrvcAccmplshmnt'] = schedule['groups']['EZPrgrmSrvcAccmplshmnt']

            # people
            if 'EZOffcrDrctrTrstEmpl' in schedule['groups']:
                xml[schedule_name]['groups']['EZOffcrDrctrTrstEmpl'] = schedule['groups']['EZOffcrDrctrTrstEmpl']
            if 'EZCmpnstnHghstPdEmpl' in schedule['groups']:
                xml[schedule_name]['groups']['EZCmpnstnHghstPdEmpl'] = schedule['groups']['EZCmpnstnHghstPdEmpl']
            if 'EZCmpnstnOfHghstPdCntrct' in schedule['groups']:
                xml[schedule_name]['groups']['EZCmpnstnOfHghstPdCntrct'] = schedule['groups']['EZCmpnstnOfHghstPdCntrct']

        elif schedule_name == 'IRS990PF':

            xml[schedule_name] = {
                'groups': {},
                'schedule_parts': {}
            }

            # org summary
            if 'pf_part_0' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['pf_part_0'] = schedule['schedule_parts']['pf_part_0']
            if 'pf_part_i' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['pf_part_i'] = schedule['schedule_parts']['pf_part_i']

            # activities
            if 'pf_part_viia' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['pf_part_viia'] = schedule['schedule_parts']['pf_part_viia']
            if 'pf_part_ixa' in schedule['schedule_parts']:
                xml[schedule_name]['schedule_parts']['pf_part_ixa'] = schedule['schedule_parts']['pf_part_ixa']

            # people
            if 'PFOffcrDrTrstKyEmpl' in schedule['groups']:
                xml[schedule_name]['groups']['PFOffcrDrTrstKyEmpl'] = schedule['groups']['PFOffcrDrTrstKyEmpl']
            if 'PFCmpnstnHghstPdEmpl' in schedule['groups']:
                xml[schedule_name]['groups']['PFCmpnstnHghstPdEmpl'] = schedule['groups']['PFCmpnstnHghstPdEmpl']
            if 'PFCmpnstnOfHghstPdCntrct' in schedule['groups']:
                xml[schedule_name]['groups']['PFCmpnstnOfHghstPdCntrct'] = schedule['groups']['PFCmpnstnOfHghstPdCntrct']

            # grants
            if 'PFGrntOrCntrApprvFrFt' in schedule['groups']:
                xml[schedule_name]['groups']['PFGrntOrCntrApprvFrFt'] = schedule['groups']['PFGrntOrCntrApprvFrFt']
            if 'PFGrntOrCntrbtnPdDrYr' in schedule['groups']:
                xml[schedule_name]['groups']['PFGrntOrCntrbtnPdDrYr'] = schedule['groups']['PFGrntOrCntrbtnPdDrYr']

            # transfers
            if 'PFRltnshpSkdDtl' in schedule['groups']:
                xml[schedule_name]['groups']['PFRltnshpSkdDtl'] = schedule['groups']['PFRltnshpSkdDtl']
            if 'PFTrnsfrSkdDtl' in schedule['groups']:
                xml[schedule_name]['groups']['PFTrnsfrSkdDtl'] = schedule['groups']['PFTrnsfrSkdDtl']

    return xml

def clean_xml(xml):
    for k, v in xml.items():
        if isinstance(v, dict):
            xml[k] = clean_xml(v)
        if isinstance(v, list):
            xml[k] = [clean_xml(i) for i in v]
        if "Amt" in k or "Hrs" in k:
            try:
                xml[k] = float(v)
            except:
                pass
        if "Dt" in k:
            try:
                xml[k] = datetime.datetime.strptime(xml[k], '%Y-%m-%d')
                xml[k] = pytz.timezone('US/Eastern').localize(xml[k])
                xml[k] = xml[k].strftime("%Y-%m-%dT%H:%M:%S%z")
            except:
                pass
    return xml
