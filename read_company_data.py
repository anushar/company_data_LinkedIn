import re
import json
import pandas as pd
#import sys
from pymongo import *
from pymongo import Connection
from pymongo.errors import ConnectionFailure
from bson.son import SON
#from datetime import datetime

try:
	c = Connection(host="localhost",port=27017)
except ConnectionFailure, e:
	sys.stderr.write("Could not connect to MongoDB: %s" %e)
	sys.exit(1)
	
dbh = c["mydb"]
	
assert dbh.connection == c
#companies = json.loads(open('BrightonCompanies'+str(i)+'.json').read())

#remove all docs from collection
dbh.companies.remove(None,safe=True)

##-------------------------Do the Brighton companies

upperLim = 79
for fileIndex in range(upperLim):
	print fileIndex
	#convert json to python dict
	db = json.loads(open('BrightonCompanies'+str(fileIndex)+'.json').read())
	#db = json.loads(open('BrightonCompanies0.json').read())
	
	#first key: "companies"
	#then three keys "_total", "_count", "_start" and "values"
	#the company data is in the values of db[u'companies'][u'values'] which is a 
	#list of dictionaries of length "_count" (20)
	
	#with this syntax db[u'companies'][u'values'][i].keys()
	#we can dig down to the actual company fields
	#i is from 0:"_count"-1

	#print db[u'companies'][u'values'][0].keys()
	#insert the companies into the db
	for company in db[u'companies'][u'values']:
		#print type(company),company.keys()
		dbh.companies.insert(company,safe=True)
		print "num companies",str(dbh.companies.find().count())	
	##unicode to ascii
	#k=companies.keys()[0]
	#k.encode('ascii','ignore')

##-------------------------Do the Hove companies
upperLim = 14
for fileIndex in range(upperLim):
	print fileIndex
	#convert json to python dict
	db = json.loads(open('HoveCompanies'+str(fileIndex)+'.json').read())
	for company in db[u'companies'][u'values']:
		dbh.companies.insert(company,safe=True)
		print "num companies",str(dbh.companies.find().count())	


#companies = dbh.companies.find().sort(("industry"))
#for company in companies:
#	print company.get('industry')
	
#dbh.companies.find().distinct('industry')
#s = rcd.dbh.companies.find({"industry":"Music"}).count()
#group by industry
#industryCount = dbh.companies.aggregate( [ { "$group" :{"_id":"$industry","total": {"$sum": 1}}}])

##-----------------filter companies by postcode
BNCompanies = dbh.companies.find({'locations.values.address.postalCode':{'$regex' :'BN*'}})

for comp in BNCompanies:
	print comp.get("name")

#all companies starting with BN
q = {'locations.values.address.postalCode':{'$regex' :'BN*'}}
#all companies not starting with BN
q = {'locations.values.address.postalCode':{'$not' : re.compile('BN*')}}
dbh.companies.remove(q)


numCompanies = dbh.companies.find({'locations.values.address.postalCode':{'$regex' :'BN*'}}).count()

##-------------------aggregations---------------------------------------------------------###

##industry
industryCount = dbh.companies.aggregate( [ { "$group" :{"_id":"$industry","total": {"$sum": 1}}},\
{"$sort": SON([("total", 1), ("_id", 1)])}])

##specialties
specialtiesCount = dbh.companies.aggregate([
         {"$unwind": "$specialties.values"},\
         {"$group": {"_id": "$specialties.values", "total": {"$sum": 1}}},\
         {"$sort": SON([("total", -1), ("_id", -1)])}
         ])

##status
statusCount = dbh.companies.aggregate( [ { "$group" :{"_id":"$status.code","total": {"$sum": 1}}},\
{"$sort": SON([("total", 1), ("_id", 1)])}])

##numFollowers
followerCount = dbh.companies.aggregate( [ { "$group" :{"_id":"$numFollowers","total": {"$sum": 1}}},\
{"$sort": SON([("total", 1), ("_id", 1)])}])

##postcode
postcodeCount = dbh.companies.aggregate([
         {"$unwind": "$locations.values.address.PostalCode"},\
         {"$group": {"_id": "$locations.values.address.PostalCode", "total": {"$sum": 1}}},\
         {"$sort": SON([("total", -1), ("_id", -1)])}
         ])

##employeeCount
employeeCountRange = dbh.companies.aggregate( [ { "$group" :{"_id":"$employeeCountRange.code","total": {"$sum": 1}}},\
{"$sort": SON([("total", 1), ("_id", 1)])}])

##year founded
yearFoundedCount = dbh.companies.aggregate( [ { "$group" :{"_id":"$foundedYear","total": {"$sum": 1}}},\
{"$sort": SON([("total", 1), ("_id", 1)])}])

##company type
companyTypeCount = dbh.companies.aggregate( [ { "$group" :{"_id":"$companyType.code","total": {"$sum": 1}}},\
{"$sort": SON([("total", 1), ("_id", 1)])}])

##search for 'ethical' or otherwise interesting terms in the description



#companyCount = dbh.companies.find().count()
#print "num companies",str(companyCount)	
	#print fileIndex
	#input()
	#print companies