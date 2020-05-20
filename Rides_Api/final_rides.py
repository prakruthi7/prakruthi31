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
app.config['MONGO_URI'] = 'mongodb://127.0.0.1:27017/RideShareApp'
api=Api(app)
mongo = PyMongo(app)
Server_IP = 'http://127.0.0.1:5000'
Users_IP = 'http://127.0.0.1:80'
UsersApi = ''

try:
    mongo.db.rides_count.insert_one({'_id':'count','value':'0'})
except:
    mongo.db.rides_count.update_one({"_id":"count"},{"$set" : {"value":"0"}})


#Ride API
class CreateRide(Resource):
#creating a ride using post method    
    def post(self):
        current_Requests = mongo.db.rides_count.find({"_id":"count"})
        updated_count=updatedRequestCount(current_Requests)
        mongo.db.rides_count.update_one({"_id":"count"},{"$set" : {"value":updated_count}})
        _json = request.json
        user =_json['created_by']
        source = _json['source']
        destination=_json['destination']
        RideID_Date = _json['timestamp']

        date_format="%d-%m-%Y:%S-%M-%H"
        print('here')

        isValidDate=True
        try:
            date_obj =datetime.datetime.strptime(RideID_Date, date_format)
        except ValueError:
            isValidDate=False
        

        isWrongData=False
        try:
            with open('static/AreaNameEnum.csv', mode='r') as infile:
                reader = csv.reader(infile)
                with open('static/AreaNameEnum_new.csv', mode='w') as outfile:
                    writer = csv.writer(outfile)
                    mydict = {rows[0]:rows[1] for rows in reader}
            current_source = mydict[source]
            current_destination = mydict[destination]

            if(len(current_source)<0 or len(current_destination)<0):
                isWrongData=True
        except:
                isWrongData=True

        if(isWrongData):
            message ={
            'status':400,
            'message':'invalid source and destination code entered in the URL'
            }
            resp = jsonify(message)
            resp.status_code=400
            return resp


        riderIDUniq=1
        if(isValidDate):
            apiResponse = requests.post(Server_IP+'/api/v1/db/read',json={"table":"rides","columns":{}})
            apiresponsedata=[]
            if(len(apiResponse.text)>0):
                apiresponsedata= json.loads(apiResponse.text)
            for rideIDData in apiresponsedata:
                if(int(rideIDData['rideID']) >= riderIDUniq):
                    riderIDUniq = int(rideIDData['rideID'])+1
            
            api_response = requests.get(Users_IP+'/api/v1/users')
            print(api_response.text)

            if (user in api_response.text):
                # _id=mongo.db.Rides.insert_one()
                _id= requests.post(Server_IP+'/api/v1/db/write',json={"table":"rides","typeofoperation":"insert","columns":"","insertdata":{'username':user,'datetime':RideID_Date,'source':source,'destination':destination,'commuters':user,'rideID':riderIDUniq}})
                resp = jsonify("Ride added successfully")
                resp.status_code=201
                return resp
            else:
                message ={
                'status':400,
                'message':'User doesnt exist , so cannot create a ride'
                }
                resp = jsonify(message)
                resp.status_code=400
                return resp
        else:
            resp = jsonify("Invalid Date Format")
            resp.status_code=400
            return resp

#displaying ride details for a specific source and destination specified in the URL query strings
    def get(self):
        current_Requests = mongo.db.rides_count.find({"_id":"count"})
        updated_count=updatedRequestCount(current_Requests)
        mongo.db.rides_count.update_one({"_id":"count"},{"$set" : {"value":updated_count}})
        
        print('heree1')
        argumentPresent = False
        for arg in request.args:
            argumentPresent = True
        

        if(not argumentPresent):
            try:
                rides_count = mongo.db.Rides.count()
                resp = jsonify('['+str(rides_count)+']')
                resp.status_code=200
                return resp
            except:
                resp = jsonify('couldnot get count')
                resp.status_code =405
                return resp

        elif(len(request.args['source'])==0 or len(request.args['destination'])==0):
            message ={
            'status':400,
            'message':'both source and destination must have a value for ride details to be displayed'
            }
            resp = jsonify(message)
            resp.status_code=400
            return resp

        else:
            arg1 = request.args['source']
            arg2 = request.args['destination']
            rideresponse = []
            isWrongData=False
            try:
                with open('static/AreaNameEnum.csv', mode='r') as infile:
                    reader = csv.reader(infile)
                    with open('static/AreaNameEnum_new.csv', mode='w') as outfile:
                        writer = csv.writer(outfile)
                        mydict = {rows[0]:rows[1] for rows in reader}

                current_source = mydict[arg1]
                current_destination = mydict[arg2]

                if(len(current_source)<0 or len(current_destination)<0):
                    isWrongData=True
            except:
                isWrongData=True

            if(isWrongData):
                message ={
                'status':400,
                'message':'invalid source and destination code entered in the URL'
                }
                resp = jsonify(message)
                resp.status_code=400
                return resp


            myquery = { 'source':arg1,'destination':arg2 }
            # documents = mongo.db.Rides.find()
            apiResponse = requests.post(Server_IP+'/api/v1/db/read',json={"table":"rides","columns":myquery})
            apiresponsedata=[]
            rideresponse = []
            
            
            if(len(apiResponse.text)>0):
                apiresponsedata= json.loads(apiResponse.text)
                print('result is'+apiResponse.text)
                for document in apiresponsedata:
                    now = datetime.datetime.now()
                    datetime_current = now.strftime("%d-%m-%Y:%S-%M-%H")
                    print('here5')
                    print('current datetime',datetime_current)
                    if(document['datetime']>datetime_current):
                        print('here4')
                        document['_id'] = str(document['_id'])
                        rideresponse.append(document)


        
            if(len(rideresponse)<1):
                print('here2222')
                message ={
                'status':204,
                'message':'No Upcoming Rides found for the given source and destination'
                }
                resp = jsonify(message)
                resp.status_code=204
                return resp
            else:
                print('hereee3')
                outputDoc = []
                for document in rideresponse:
                    docID = document['_id']
                    docName = document['username']
                    docTime = document['datetime']
                    docRideID = document['rideID']
                    outputDoc.append({'rideId':docRideID,'username':docName,'timestamp':docTime},)
                resp = jsonify(outputDoc)
                resp.status_code=200
            return resp

class ModifyRide(Resource):
    def get(self,rideid):
        current_Requests = mongo.db.rides_count.find({"_id":"count"})
        updated_count=updatedRequestCount(current_Requests)
        mongo.db.rides_count.update_one({"_id":"count"},{"$set" : {"value":updated_count}})

        myquery = {"rideID":rideid}
        tempRide=''
        response=[]
        
        apiresponse = requests.post(Server_IP+'/api/v1/db/read',json={"table":"rides","columns":myquery})
        if(len(apiresponse.text)>0):
            apiResponseData=json.loads(apiresponse.text)
            for doc in apiResponseData:
                tempRide = [doc['_id']]
                response.append(doc)
        
        print('output',tempRide)
        if (len(tempRide) >0):
            outputDoc = []
            for eachRide in response:
                    RidecreatedName = eachRide['username']
                    Rideusers=eachRide['commuters']
                    rideTimestamp=eachRide['datetime']
                    rideSource=eachRide['source']
                    rideDestination=eachRide['destination']
                    rideID = eachRide['rideID']
                    outputDoc.append({'rideId':rideID,'created_by':RidecreatedName ,'users':Rideusers,'timestamp':rideTimestamp,'source':rideSource,'destination':rideDestination},)
            return outputDoc
        else:
            message ={
            'status':400,
            'message':'Ride doesnt exist'
            }
            resp = jsonify(message)
            resp.status_code=400
            return resp

    def post(self,rideid):
        current_Requests = mongo.db.rides_count.find({"_id":"count"})
        updated_count=updatedRequestCount(current_Requests)
        mongo.db.rides_count.update_one({"_id":"count"},{"$set" : {"value":updated_count}})

        ridequery = {'rideID':rideid}
        tempRide=''
        response=[]

        _json = request.json
        user =_json['username']

        userquery = { 'user':user }
        
        usersApi_response = requests.get(Users_IP+'/api/v1/users')
        print('apiresponse'+usersApi_response.text)
     
        apiresponse = requests.post(Server_IP+'/api/v1/db/read',json={"table":"rides","columns":ridequery})
        if(len(apiresponse.text)>0):
            apiResponseData=json.loads(apiresponse.text)
            for doc in apiResponseData:
                tempRide = [doc['_id']]
                response.append(doc)
        


        if (usersApi_response.text.find(user)!=-1):
            if(len(tempRide)>0):
                users = user
                for eachride in response:
                    users = users+ ',' +eachride['commuters']
                    print('commuters new field',users)
                    updateresponse = requests.post(Server_IP+'/api/v1/db/write',json={"table":"rides","typeofoperation":"update","columns":{'rideID':rideid},"insertdata":{'commuters': users}})
                resp = jsonify(updateresponse.text)
                resp.status_code = 201
                return resp
            else:
                message ={
                'status':400,
                'message':'Ride doesnt exist'
                }
                resp = jsonify(message)
                resp.status_code=400
                return resp
        else:
            message ={
            'status':400,
            'message':'cannot update the Ride , invalid user'
            }
            resp = jsonify(message)
            resp.status_code=400
            return resp


    def delete(self,rideid):
        current_Requests = mongo.db.rides_count.find({"_id":"count"})
        updated_count=updatedRequestCount(current_Requests)
        mongo.db.rides_count.update_one({"_id":"count"},{"$set" : {"value":updated_count}})

        myquery = {'rideID':rideid}
        currentRide=''


        apiresponse = requests.post(Server_IP+'/api/v1/db/read',json={"table":"rides","columns":myquery})
        if(len(apiresponse.text)>0):
            apiResponseData=json.loads(apiresponse.text)
            for doc in apiResponseData:
                currentRide = [doc['_id']]
                
        
        if (len(currentRide) >0):
            result = requests.post(Server_IP+'/api/v1/db/write',json={"table":"rides","typeofoperation":"delete","insertdata":"","columns":myquery})
            print('result value is ' , result)
            message ={
            'status':200,
            'message':'Ride deleted successfully'
            }
            resp = jsonify(message)
            resp.status_code=200
            return resp
        else:
            message ={
            'status':400,
            'message':'Ride doesnt exist'
            }
            resp = jsonify(message)
            resp.status_code=400
            return resp


class ClearDB(Resource):
    def post(self):
        try:
            mongo.db.Rides.remove()    
            resp = jsonify("Rides removed successfully")
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
            current_Requests = mongo.db.rides_count.find({"_id":"count"})
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
            mongo.db.rides_count.update_one({"_id":"count"},{"$set" : {"value":"0"}})
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



api.add_resource(CreateRide,'/api/v1/rides')
api.add_resource(ModifyRide,'/api/v1/rides/<int:rideid>')
api.add_resource(ClearDB,'/api/v1/db/clear')
api.add_resource(ApiCount,'/api/v1/_count','/api/v1/rides/count')
api.add_resource(index,'/index.html')


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0',port=8050)