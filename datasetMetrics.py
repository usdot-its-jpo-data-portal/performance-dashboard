import time
import datetime
import requests
from requests.auth import HTTPBasicAuth
requests.packages.urllib3.disable_warnings()
from sodapy import Socrata
import yaml
import psycopg2
import httplib2
import os

from apiclient.http import MediaFileUpload
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

'''
This code access the Socrata API to select dataset downloads, views, API downloads
and API views for ITS JPO datasets on data.transportation.gov, then appends that
data to a Postgres database and writes monthly numbers to a Google Sheets object
through the Google Drive API. 

Requires:
- Postgres database with table dtg_metrics and columns
dataset_name,views_by_month,monthly_downloads,api_access,api_downloads,downloads,end_date
Code is currently set up to work with ipdh_metrics.dtg_metrics
- Google Sheets object with column headers: dataset_name,views_by_month,monthly_downloads,api_access,api_downloads
- client_secret.json, follow Google Drive API instructions for acquiring client_secret.json.
- data.transportation.gov account with Discovery API access.
- soda.yml file including data.transporation.gov username and password.
'''

SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def unix_time_millis(dt):
    return int(datetime.datetime.timestamp(dt)*1000)

def getDatasetMetrics(dataset_id):
    url = "https://data.transportation.gov/api/views/" + dataset_id + "/metrics.json?" + "start=" + str(unix_time_millis(start_date)) + "&end=" + str(unix_time_millis(end_date)) + "&method=series&slice=MONTHLY"
    r = requests.get(url,auth=HTTPBasicAuth(socrata_config["username"], socrata_config["password"]))
    r = r.json()
    return r[0]['metrics']['rows-accessed-api'], r[0]['metrics']['rows-loaded-api']

end_date = datetime.datetime.combine(datetime.date.today(),datetime.time(tzinfo=datetime.timezone(datetime.timedelta(0))))
start_date = end_date - datetime.timedelta(days=28)

with open("soda.yml", 'r') as stream:
    socrata_config = yaml.load(stream)

r = requests.get("https://api.us.socrata.com/api/catalog/v1?domains=data.transportation.gov&tags=its+joint+program+office+(jpo)&search_context=data.transportation.gov", auth=HTTPBasicAuth(socrata_config["username"], socrata_config["password"]))
r = r.json()

value_range_body = {'values':[]}
#Add parameters to connect to specific Postgres database
conn = psycopg2.connect("")
cur = conn.cursor()
cur.execute("SET TIME ZONE 'UTC'")
for dataset in r['results']:
    dataset_name = dataset['resource']['name']
    views_by_month = dataset['resource']['page_views']['page_views_last_month']
    downloads = dataset['resource']['download_count']
    try:    
        cur.execute("SELECT downloads FROM ipdh_metrics.dtg_metrics WHERE datetime = %s and dataset_name = %s",(end_date,dataset_name))
        previous_total = cur.fetchone()[0]
    except:
        previous_total = 0
    monthly_downloads = downloads - previous_total
    api_access, api_downloads = getDatasetMetrics(dataset["resource"]["id"])
    cur.execute("INSERT INTO ipdh_metrics.dtg_metrics VALUES (%s,%s,%s,%s,%s,%s,%s)",(dataset_name,views_by_month,monthly_downloads,api_access,api_downloads,downloads,end_date))
    value_range_body['values'].append([dataset_name,views_by_month,monthly_downloads,api_access,api_downloads])
conn.commit()
cur.close()
conn.close()

credentials = get_credentials()
http = credentials.authorize(httplib2.Http())
service = discovery.build('sheets', 'v4', credentials=credentials)
#Enter spreadsheet id from Google Sheets object
#spreadsheet_id = ""
spreadsheetRange = "A2:E" + str(len(r['results']) + 1)
value_input_option = 'USER_ENTERED'
request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=spreadsheetRange, valueInputOption=value_input_option, body=value_range_body)
response = request.execute()
print(response)