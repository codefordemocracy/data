import requests
import time
from dateutil.parser import parse
from irsx.xmlrunner import XMLRunner
from filing_parsers_990pf import grants_from_990pf, org_from_990pf, timestamp_now
from filing_parsers_990 import grants_from_990, org_from_990
from filing_parsers_990ez import org_from_990ez # 990EZ filers don't have grants

from flask import Flask, request
from urllib.parse import urlencode
import simplejson
from pymongo import MongoClient, ReturnDocument
from bson.dbref import DBRef

app = Flask(__name__)

API_KEY = 'XXXXXXXXXXXXXXXX'
MONGO_URL = 'mongodb+srv://XXXXXXXXXXXXXXXX:XXXXXXXXXXXXXXXX@XXXXXXXXXXXXXXXX'

@app.route('/process_local')
def process_local():
    return process(request)

def process(request):
    if request.headers.get('x-api-key', '') != API_KEY:
        return 'Not found', 404

    xml_runner = XMLRunner()
    try:
        filing = xml_runner.run_filing(request.args.get('aws_object_id',''))
    except RuntimeError as e:
        return "Error getting XML: {0}".format(str(e)), 400

    try:
        if 'IRS990PF' in filing.list_schedules():
            org = org_from_990pf(filing)
            grants_to_create = grants_from_990pf(filing)
        elif 'IRS990EZ' in filing.list_schedules():
            org = org_from_990ez(filing)
            grants_to_create = []
        elif 'IRS990' in filing.list_schedules():
            org = org_from_990(filing)
            grants_to_create = grants_from_990(filing)
        else:
            raise RuntimeError('No schedule available to parse.')
    except RuntimeError as e:
        return "Error getting org: {0}".format(str(e)), 500

    if org.get('ein', '') == '':
        return "No EIN found", 500

    client = MongoClient(MONGO_URL)
    db = client.springsteen

    timestamp = timestamp_now()
    org['updatedAt'] = timestamp

    existing_org = db.organizations.find_one({'ein': org['ein']})
    if existing_org == None:
        org['createdAt'] = timestamp
        result = db.organizations.insert_one(org)
        org_mongo_id = result.inserted_id
    else:
        org_mongo_id = existing_org['_id']
        if 'lastFilingAt' not in existing_org or parse(existing_org['lastFilingAt']) < parse(org['lastFilingAt']):
            merged_org = {**existing_org, **org}
            if 'createdAt' not in merged_org or merged_org['createdAt'] == 'yo':
                merged_org['createdAt'] = timestamp
            result = db.organizations.find_one_and_update({'_id': existing_org['_id']}, {'$set': merged_org}, return_document=ReturnDocument.AFTER)

    for grant in grants_to_create:
        grant['funder'] = DBRef('organizations', org_mongo_id)
        grant['createdAt'] = timestamp
        grant['updatedAt'] = timestamp

    if len(grants_to_create) > 0:
        # Grants should not be replaced if they are already uploaded for that tax period/funder since they can be modified by other sources after initial upload
        if db.grants.find_one({'funderEIN': org['ein'], 'fromTaxPeriodEnding': grants_to_create[0]['fromTaxPeriodEnding']}) == None:
            result = db.grants.delete_many({'funderEIN': org['ein'], 'fromTaxPeriodEnding': grants_to_create[0]['fromTaxPeriodEnding']})
            result = db.grants.insert_many(grants_to_create)

    return 'OK'
