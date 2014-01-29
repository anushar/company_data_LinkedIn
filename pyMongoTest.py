import sys
from pymongo import *
from pymongo import Connection
from pymongo.errors import ConnectionFailure
from datetime import datetime



print "here"
try:
	c = Connection(host="localhost",port=27017)
except ConnectionFailure, e:
	sys.stderr.write("Could not connect to MongoDB: %s" %e)
	sys.exit(1)
	
dbh = c["mydb"]
	
assert dbh.connection == c

#add some users
user_doc = {"username" : "vasilishatzopoulos","name":"Vasilis","surname":"Hatzopoulos"\
,"dob":datetime(1978,11,03),"score":"1"}
#safe=True ensures that the write is succesfull or an exception will be thrown
dbh.users.insert(user_doc,safe=True)
print "successfully inserted documents: %s" %user_doc

user_doc = {"username" : "hannahfaux","name":"Hannah","surname":"Faux","dob":datetime(1983,02,10),"score":"1"}
dbh.users.insert(user_doc,safe=True)
print "successfully inserted documents: %s" %user_doc

user_doc = {"username" : "hannahrobbins","name":"Hannah","surname":"Robbins","dob":"15/06/84","score":"0"}
dbh.users.insert(user_doc,safe=True)
print "successfully inserted documents: %s" %user_doc

#find one user
"""
user_doc = dbh.users.find_one({"username":"hannahfaux"})
if not user_doc:
	print "user not found"
else:
	print "user found"
"""
#find all user with name Hannah and print their surname
"""
users = dbh.users.find({"name":"Hannah"})
for user in users:
	#print user.get("surname")
	print user["surname"]
"""
#users = dbh.users.find({"name":"Hannah"}).sort(("dob",limit(5)))
users = dbh.users.find({"name":"Vasilis"},sort=[("dob",DESCENDING)])
for user in users:
	print user.get("dob")
	#print user["surname"], user["dob"]

#update a value using update modiffier $set, this avoids race conditions
dbh.users.update({"name":"Vasilis"},{"$set":{"email":"vasilis.hatzopoulos@yahoo.co.uk"}},safe=True)
#this only updates a single document even if the spec matches many documents
#set multi=True to update all

usercount = dbh.users.find().count()
print "num users",str(usercount)

#remove document from collection
dbh.users.remove({"name":"Vasilis"},safe=True)

#remove all docs from collection
dbh.users.remove(None,safe=True)
#count the number of users

usercount = dbh.users.find().count()
print "num users",str(usercount)

#a user document demonstrating one-to-many-relationsships using embeding
user_doc = {"username":"vas", \
"emails":[{"email":"vhatzopoulos@gmail.com"},{"email":"vasilis.hatzopoulos@yahoo.co.uk"}]}
dbh.users.insert(user_doc,safe=True)

#querying for embbeded values via the dot operator by retrieving the just inserted
#document using one of its many addresses
query_result = dbh.users.find_one({"emails.email":"vhatzopoulos@gmail.com"})

#print query_result.get("emails")

assert user_doc == query_result

if __name__ == "__main__":
	main()

