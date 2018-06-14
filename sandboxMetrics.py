import boto3
import yaml
import psycopg2
import httplib2
import os
import time
import datetime

from apiclient.http import MediaFileUpload
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

pageviews = 0
wydot_bsm_downloads = 0
wydot_tim_downloads = 0
tampa_bsm_downloads = 0
tampa_spat_downloads = 0
nyc_bsm_downloads = 0
nyc_spat_downloads = 0
nyc_map_downloads = 0

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

'''
This code accesses an Amazon Web Services s3 bucket that stores
Amazon Web Services access logs to calculate data access and downloads
for the public, ITS JPO distributed CV Pilot datasets on the ITS Public Data Hub Sandbox, 
then appends that data to a Postgres database and writes monthly numbers to a Google Sheets object
through the Google Drive API. 

Requires:
- Postgres database with table sandbox_metrics and columns
datetime,pageviews,wydot_bsm_downloads,wydot_tim_downloads,tampa_bsm_downloads,
tampa_spat_downloads,nyc_bsm_downloads,nyc_spat_downloads,nyc_map_downloads
Code is currently set up to work with ipdh_metrics.sandbox_metrics
- Google Sheets object with column headers: dataset_name,views_by_month,monthly_downloads,api_access,api_downloads
- client_secret.json, follow Google Drive API instructions for acquiring client_secret.json.
- application name for Google API approval 
- Amazon Web Services account with read access to AWS s3 access log bucket
- Access logs enabled for bucket storing data 
'''

SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'APPLICATION_NAME'

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

def process_lines(log):
    '''
    Reads each line in log file. Looks for keyword REST.GET.OBJECT that indicates a file was downloaded by a user. Checks which file was
    downloaded/accessed by the user and adds to the appropriate count.
    '''
    global pageviews,wydot_bsm_downloads,wydot_tim_downloads,tampa_bsm_downloads,tampa_spat_downloads,nyc_bsm_downloads,nyc_spat_downloads,nyc_map_downloads
    for line in log:
        if "REST.GET.OBJECT" in line:
            row = line.split(" ")
            item = row[row.index("REST.GET.OBJECT") + 1]
            if item == "index.html":
                pageviews += 1
            elif "wydot" in item:
                if "BSM" in item:
                    wydot_bsm_downloads += 1
                else:
                    wydot_tim_downloads += 1
            elif "tampa" in item:
                if "BSM" in item:
                    tampa_bsm_downloads += 1
                else:
                    tampa_spat_downloads += 1
            elif "nyc" in item:
                if "BSM" in item:
                    nyc_bsm_downloads += 1
                elif "SPAT" in item:
                    nyc_spat_downloads += 1
                else:
                    nyc_map_downloads += 1

def get_monthly(cur, today):
    '''
    Queries database to get past month of data to write to Google Sheets for dashboard to access
    '''
    last_month = today - datetime.timedelta(days=29)
    cur.execute("SELECT datetime,pageviews,wydot_bsm_downloads,wydot_tim_downloads,tampa_bsm_downloads,tampa_spat_downloads,nyc_bsm_downloads,nyc_spat_downloads,nyc_map_downloads FROM ipdh_metrics.sandbox_metrics WHERE datetime >= %s",[last_month])
    results = cur.fetchall()
    value_range_body = {'values':[]}
    for record in results:
        row = []
        row.append(record[0].strftime("%Y-%m-%d %H:%M:%S"))
        row.append(record[1])
        row.append(record[2])
        row.append(record[3])
        row.append(record[4])
        row.append(record[5])
        row.append(record[6])
        row.append(record[7])
        row.append(record[8])
        value_range_body['values'].append(row)
    return value_range_body

session = boto3.session.Session()
s3 = session.resource('s3')
#Add s3 bucket name that contains server access log files
mybucket = s3.Bucket('')

today = datetime.datetime.combine(datetime.date.today(),datetime.time(tzinfo=datetime.timezone(datetime.timedelta(0))))
yesterday = today - datetime.timedelta(hours=24)
for record in mybucket.objects.filter(Prefix="logs/"):
        if record.last_modified > yesterday and record.last_modified <= today:
            log = record.get()['Body'].read().decode('utf-8')
            log = log.splitlines()
            process_lines(log)

#Add parameters to connect to specific Postgres database
conn = psycopg2.connect("")
cur = conn.cursor()
cur.execute("SET TIME ZONE 'UTC'")
cur.execute("INSERT INTO ipdh_metrics.sandbox_metrics (datetime,pageviews,wydot_bsm_downloads,wydot_tim_downloads,tampa_bsm_downloads,tampa_spat_downloads,nyc_bsm_downloads,nyc_spat_downloads,nyc_map_downloads) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",(today,pageviews,wydot_bsm_downloads,wydot_tim_downloads,tampa_bsm_downloads,tampa_spat_downloads,nyc_bsm_downloads,nyc_spat_downloads,nyc_map_downloads))
value_range_body = get_monthly(cur, today)
conn.commit()
cur.close()
conn.close()
 
credentials = get_credentials()
http = credentials.authorize(httplib2.Http())
service = discovery.build('sheets', 'v4', credentials=credentials)
#Enter spreadsheet id from Google Sheets object
spreadsheet_id = ""
spreadsheetRange = "A2:I" + str(len(value_range_body['values']) + 1)
value_input_option = 'USER_ENTERED'
request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=spreadsheetRange, valueInputOption=value_input_option, body=value_range_body)
response = request.execute()
print(response)