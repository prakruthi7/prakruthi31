import json
from flask import Flask
from flask_pymongo import PyMongo
from flask_restful import Resource, Api
from flask import request,jsonify
import os
from werkzeug.security import generate_password_hash,check_password_hash
import datetime
import csv
from webfunctions import ProcessData,updatedRequestCount
import requests



app = Flask(__name__)
app.config['MONGO_URI'] = 'mongodb://prakruthi:pakku123@cluster0-shard-00-00-lh5wa.mongodb.net:27017,cluster0-shard-00-01-lh5wa.mongodb.net:27017,cluster0-shard-00-02-lh5wa.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority'
api=Api(app)
mongo = PyMongo(app)
Server_IP = 'http://54.224.5.20'
try:
    mongo.db.users_count.insert_one({'_id':'count','value':'0'})
except:
    mongo.db.users_count.update_one({"_id":"count"},{"$set" : {"value":"0"}})
    

#Ride Sharing APP

# class JSONEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, ObjectId):
#             return str(o)
#         return json.JSONEncoder.default(self, o)

#User API 
class User(Resource):
#Add user

    def put(self):

        current_Requests = mongo.db.users_count.find({"_id":"count"})
        updated_count=updatedRequestCount(current_Requests)
        mongo.db.users_count.update_one({"_id":"count"},{"$set" : {"value":updated_count}})

        apiResponse = requests.post(Server_IP+'/api/v1/db/read',json={"table":"users","columns":""})
        
        _json = request.json
        user =_json['username']
        _password=_json['password']

        myquery = { 'user': user }
        users=[]

        apiResponse = requests.post(Server_IP+'/api/v1/db/read',json={"table":"users","columns":myquery})
        apiresponsedata=[]
        if(len(apiResponse.text)>0):
            apiresponsedata= json.loads(apiResponse.text)

            for doc in apiresponsedata:
            # append each document's ID to the list
                users.append([doc['_id']])    


        if(len(users)==0):
            if user and _password and request.method =='PUT':
               _hashed_password = generate_password_hash(_password)
               id =requests.post(Server_IP+'/api/v1/db/write',json={"table":"users","typeofoperation":"insert","columns":"","insertdata":{'user':user,'password':_hashed_password}})
               # id = mongo.db.users.insert()
               resp = jsonify("User added successfully")
               resp.status_code=201
               return resp
            else:
               message ={
               'status':400,
               'message':'User addition not possible'
                }
               resp = jsonify(message)
               resp.status_code=400
               return resp
        else:
            message ={
            'status':400,
            'message':'User already exists'
             }
            resp = jsonify(message)
            resp.status_code=400
            return resp
#Delete user   
    def delete(self , name):
        current_Requests = mongo.db.users_count.find({"_id":"count"})
        updated_count=updatedRequestCount(current_Requests)
        mongo.db.users_count.update_one({"_id":"count"},{"$set" : {"value":updated_count}})

        myquery = { 'user':name }
        users=[]

        apiResponse = requests.post(Server_IP+'/api/v1/db/read',json={"table":"users","columns":myquery})
        print('apiresponse is :'+apiResponse.text)

        apiresponsedata=[]
        if(len(apiResponse.text)>0):
            apiresponsedata= json.loads(apiResponse.text)

            for doc in apiresponsedata:
                users.append([doc['_id']]) 


        if (len(users) >0):
            result =requests.post(Server_IP+'/api/v1/db/write',json={"table":"users","typeofoperation":"delete","columns":myquery,"insertdata":""})
            print('result value is ' , result.text)
            message ={
            'status':200,
            'message':'User deleted successfully'
            }


            resp = jsonify(message)
            resp.status_code=200
            return resp
        else:
            message ={
            'status':400,
            'message':'User doesnt exist'
            }
            resp = jsonify(message)
            resp.status_code=400
            return resp        
#get all the user details
    def get(self):
        current_Requests = mongo.db.users_count.find({"_id":"count"})
        updated_count=updatedRequestCount(current_Requests)
        mongo.db.users_count.update_one({"_id":"count"},{"$set" : {"value":updated_count}})
        # append each document's ID to the list    
        apiResponse = requests.post(Server_IP+'/api/v1/db/read',json={"table":"users","columns":""})
        
        response = []
        apiresponsedata=[]
        if(len(apiResponse.text)>0):
            apiresponsedata= json.loads(apiResponse.text)

            for document in apiresponsedata:
                document['_id'] = str(document['_id'])
                response.append(document)

        
        if(len(response)==0):
             resp = jsonify("No Users are present")
             resp.status_code=204
             return resp
        else:
            outputDoc = []
            for document in response:
                    docID = document['_id']
                    docName = document['user']
                    outputDoc.append({'userID':docID,'UserName':docName},)
            return outputDoc



class ClearDB(Resource):
    def post(self):
        try:
            mongo.db.users.remove()    
            resp = jsonify("users removed successfully")
            resp.status_code=201
            return resp
        except:
            resp = jsonify("couldnot clear the database")
            resp.status_code=400
            return resp

class ApiCount(Resource):
    def get(self):
        try:
            current_count=0
            current_Requests = mongo.db.users_count.find({"_id":"count"})
            for rec in current_Requests:
                current_count = int(rec['value'])
            resp = jsonify('['+str(current_count)+']')
            resp.status_code=200
            return resp
        except:
            resp = jsonify("couldnot get the count")
            resp.status_code=405
            return resp
        
    def delete(self):
        try:
            mongo.db.users_count.update_one({"_id":"count"},{"$set" : {"value":"0"}})
            resp = jsonify("succesfully reset the count to 0 ")
            resp.status_code=200
            return resp
        except:
            resp = jsonify("couldnot update the count")
            resp.status_code=405
            return resp

class index(Resource):
    def get(self):
        resp = jsonify("server is up and running")
        resp.status_code=200
        return resp


#routing info

api.add_resource(User,'/api/v1/users','/api/v1/users/<string:name>','/api/v1/users/display')
api.add_resource(ClearDB,'/api/v1/db/clear')
api.add_resource(ApiCount,'/api/v1/_count')
api.add_resource(index,'/index.html')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0',port=80)