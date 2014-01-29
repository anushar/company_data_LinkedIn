import csv
from linkedin import linkedin
import json


filename = 'application_details.csv'
f = open(filename,'r')
reader = csv.reader(f)

appDetails = {}
for line in reader:
	appDetails[line[0]] = line[1]
	
RETURN_URL = ''
CONSUMER_KEY = appDetails['API Key']
CONSUMER_SECRET = appDetails['Secret Key']
USER_TOKEN = appDetails['OAuth User Token']
USER_SECRET = appDetails['OAuth User Secret']
auth = linkedin.LinkedInDeveloperAuthentication(CONSUMER_KEY, CONSUMER_SECRET, \
												USER_TOKEN, USER_SECRET, \
												RETURN_URL, \
												permissions=linkedin.PERMISSIONS.enums.values())
												
app = linkedin.LinkedInApplication(auth)


#selectors
sel = [{'companies': ['name', 'universal-name', 'website-url',\
'industry','company-type','status','blog-rss-url','twitter-id',\
'employee-count-range','specialties','locations','description',\
'num-followers','founded-year','end-year']}]

#par ={'keywords': 'Brighton','start':20,'count':20}
#par = {'keywords': 'Brighton','hq-only':'Brighton','start':0,'count':5}

#count = 20
#pageLims = range(0,1720,count)

count = 20
pageLims = range(0,306,count)

#get the 20 companies per call


for i in range(len(pageLims)-1):
	
	start = pageLims[i]
	print i,pageLims[i],pageLims[i+1]
	#par = {'keywords': 'Brighton','hq-only':true,'start':start,'count':count}
	#par = {'keywords': 'Brighton','start':start,'count':count}
	par = {'keywords': 'Hove','start':start,'count':count}
	#par = {'facet': 'location,London','hq-only':True,'start':start,'count':count}
	companies = app.search_company(selectors=sel, params=par)
	#companies_data = 'BrightonCompanies'+ str(i)  +'.json'
	companies_data = 'HoveCompanies'+ str(i)  +'.json'
	f = open(companies_data, 'a')
	f.write(json.dumps(companies,indent=1))
	f.close()
	print json.dumps(companies, indent=1)
	#input()
	#print 'start '+str(i)
	
