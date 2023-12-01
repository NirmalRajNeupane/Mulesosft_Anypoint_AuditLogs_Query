import logging
import requests
import datetime
import json

BASE_URL = "https://anypoint.mulesoft.com"
TIME_STRING = "%Y-%m-%dT%H:%M:%S.000Z"
MAX_RECORDS = 1000000

logger = logging.getLogger(__name__)

def get_credentials():
    credential_file = open("credentials.json")
    credentials = json.load(credential_file)
    return_dict = {
        "org_id": credentials["org_id"],
        "client_id": credentials["client_id"],
        "client_secret": credentials["client_secret"] 
    }
    return return_dict

def get_checkpoint():
    try:
        checkpoint_file = open("checkpoint.json")
        #TODO upate the catchall exception to specific one
    except Exception as e:
        logger.info("No previous checkpoints found")
        return False
    
    checkpoint_data = json.load(checkpoint_file)
    checkpoint_datetime = datetime.datetime.strptime(checkpoint_data["checkpoint_date"],TIME_STRING)
    logger.info(f"Previous checkpoint date: {checkpoint_data['checkpoint_date']} found")
    return checkpoint_datetime

def save_checkpoint(checkpoint_datetime):
    checkpoint_data = {"checkpoint_date": checkpoint_datetime.strftime(TIME_STRING)}
    checkpoint_data = json.dumps(checkpoint_data, indent=2)

    with open("checkpoint.json", "w+") as checkpoint_file:
        checkpoint_file.write(checkpoint_data)

def get_bearer_token(client_id, client_secret):
    login_url = f'{BASE_URL}/accounts/api/v2/oauth2/token'
    login_payload = { 
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }

    login_headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    
    login_response = requests.post(login_url, data = login_payload, headers=login_headers)
    login_response.raise_for_status()
    return login_response.json()["access_token"]


def get_audit_logs(access_token, startDate, endDate, offset, page_size, org_id):
    audit_url = f'{BASE_URL}/audit/v2/organizations/{org_id}/query'
    
    audit_request_payload = {
        "startDate":startDate,
        "endDate":endDate,
        "platforms":[],
        "objectTypes":[],
        "actions":[],
        "environmentIds":[],
        "objectIds":[],
        "userIds":[],
        "offset":offset,
        "limit":page_size,
        "ascending":False
    }

    audit_header = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    audit_response = requests.post(audit_url, data = json.dumps(audit_request_payload), headers = audit_header)
    audit_response.raise_for_status()
    return audit_response.json()


credentials = get_credentials()
access_token = get_bearer_token(credentials['client_id'], credentials['client_secret'])

today = datetime.datetime.now()

start_time = get_checkpoint()
if not start_time:
    logger.info("Setting checkpoint to 90days in the past from now")
    start_time = today - datetime.timedelta(days=90)

page_size = 200
has_more = True
offset=0
data = []
while has_more:
    audit_logs = get_audit_logs(access_token, start_time.strftime(TIME_STRING), today.strftime(TIME_STRING), offset, page_size, credentials["org_id"])
    total = audit_logs['total']
    offset = offset + page_size
    has_more = (offset < total) and offset < MAX_RECORDS
    data.extend(audit_logs["data"])

save_checkpoint(today)

print(json.dumps(data, indent=2))
