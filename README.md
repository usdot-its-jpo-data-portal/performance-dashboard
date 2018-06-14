# Performance Dashboard
This repository includes the three scripts used by the USDOT team to access performance metrics for data.transportation.gov, Amazon Web Services s3 and GitHub and upload them to two separate locations. The first is an internal Postgres database that will keep an all time historical record. The second is a Google Sheets object which keeps a running monthly total that are accessed by Google Data Studio to create the performance dashboard on the ITS Public Data Hub. This process is designed to be run daily. These scripts have been stripped of personal information and therefore will not run as-is and require information as noted in the files themselves. All code is written for Python 3.6. 

For more information on the Google Drive Web API: https://developers.google.com/drive/v3/web/about-sdk

For more information on psycopg2: http://initd.org/psycopg/docs/

For more information on GitHub Traffic API: https://developer.github.com/v3/repos/traffic/
