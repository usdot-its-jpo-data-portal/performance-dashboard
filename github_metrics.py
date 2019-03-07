import requests
from requests.auth import HTTPBasicAuth
import psycopg2
import httplib2
import os
import time
import datetime

import yaml

from apiclient.http import MediaFileUpload
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

from sesemail import sendEmail

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

'''
This code access the GitHub API to select the traffic and views, then appends that 
data to a Postgres database and writes monthly numbers to a Google Sheets object
through the Google Drive API. 
Requires:
- Postgres database with table github_metrics and columns
id autoincrement,repository,datetime,count,uniques
Code is currently set up to work with ipdh_metrics.github_metrics
- Google Sheets object with column headers: repository,datetime,count,uniques
- client_secret.json, follow Google Drive API instructions for acquiring client_secret.json.
- application name for Google API approval 
- GitHub account.
'''

SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'its-jpo-metrics'

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

def make_request(repository, username, password, cur):
    r = requests.get("https://api.github.com/repos/usdot-its-jpo-data-portal/{}/traffic/views".format(repository), auth=HTTPBasicAuth(username,password))
    r = r.json()
    today = datetime.date.today() - datetime.timedelta(days=1)
    notoday = True
    for row in r["views"]:
        timestamp = row["timestamp"]
        timestamp = datetime.datetime.strptime(timestamp,"%Y-%m-%dT%H:%M:%SZ").date()
        if timestamp == today:
            notoday = False
            count = row["count"]
            unqiues = row["uniques"]
            cur.execute("INSERT INTO ipdh_metrics.github_metrics (repository,datetime,count,uniques) VALUES (%s,%s,%s,%s)", (repository,timestamp,count,unqiues))
    if notoday:
        cur.execute("INSERT INTO ipdh_metrics.github_metrics (repository,datetime,count,uniques) VALUES (%s,%s,0,0)", (repository,today))

def get_monthly(repository,cur,service):
    today = datetime.datetime.combine(datetime.date.today(),datetime.time(tzinfo=datetime.timezone(datetime.timedelta(0))))
    last_month = today - datetime.timedelta(days=28)
    cur.execute("SELECT repository,datetime,count,uniques FROM ipdh_metrics.github_metrics WHERE repository = %s AND datetime >= %s",(repository,last_month))
    results = cur.fetchall()
    for record in results:
        row = []
        row.append(record[0])
        row.append(record[1].strftime("%Y-%m-%d %H:%M:%S"))
        row.append(record[2])
        row.append(record[3])
        value_range_body['values'].append(row)

try:

    with open("config.yml", 'r') as stream:
        config = yaml.load(stream)

    #Set username and password for GitHub account
    username = config["github_username"]
    password = config["github_password"]
    conn = psycopg2.connect(config["pg_connection_string"])
    cur = conn.cursor()
    cur.execute("SET TIME ZONE 'UTC'")
    make_request("sandbox", username, password, cur)
    make_request("microsite", username, password, cur)
    conn.commit()
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('sheets', 'v4', credentials=credentials)
    value_range_body = {'values':[]}
    get_monthly("sandbox", cur, service)
    get_monthly("microsite", cur, service)
    cur.close()
    conn.close()
    #Enter spreadsheet id from Google Sheets object
    spreadsheet_id = config["spreadsheet_id_github"]
    spreadsheetRange = "A2:E57"
    value_input_option = 'USER_ENTERED'
    request = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=spreadsheetRange, valueInputOption=value_input_option, body=value_range_body)
    response = request.execute()
    print(response)

except Exception as e:
    sendEmail("Github", str(e))