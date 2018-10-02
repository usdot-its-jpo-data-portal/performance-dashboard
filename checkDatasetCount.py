import requests
from requests.auth import HTTPBasicAuth
requests.packages.urllib3.disable_warnings()
import json

NTL_url = "https://rosap.ntl.bts.gov/fedora/export/view/collection/";
NTL_collection = "dot:239";
NTL_datelimit = "?from=2018-01-01T00:00:00Z";
NTL_rowslimit = "&rows=9999";

r = requests.get(NTL_url + NTL_collection + NTL_datelimit + NTL_rowslimit)
r = r.json()
ntlout = []
for row in r['response']['docs']:
	if row["mods.sm_resource_type"][0] == 'Dataset':
		ntlout.append(ascii(row["dc.title"][0]))

with open("/home/johara/metrics/NTL.txt", 'r') as in_f:
	header = True
	for line in in_f:
		if header:
			if int(line) == len(ntlout):
				break
			else:
				print("ALERT!! Number of NTL datasets has changed. Previously there were {0} datasets, now there are {1}.".format(int(line),len(ntlout)))
				header = False
				continue
		name = line.strip("\n")
		if name in ntlout:
			ntlout.remove(name)
			continue
		else:
			print(name + " was not found in latest check.")
	if not header:
		for item in ntlout:
			print(item + " was not previously listed.")
print("\n")