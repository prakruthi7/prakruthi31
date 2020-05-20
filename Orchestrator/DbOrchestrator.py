import os
import sys
import json
import logging
from flask import Flask
from flask_restful import Resource, Api
from flask import request,jsonify
import os
from werkzeug.security import generate_password_hash,check_password_hash
import datetime
import csv
from webfunctions import ProcessData,noOfRecordsAffected,read_request,ProcessOutputdata
import requests
from logging import FileHandler,WARNING
from flask_pymongo import PyMongo
from pymongo import MongoClient
from bson.json_util import dumps
import subprocess
import pika
# import concurrent.futures

app = Flask(__name__)
app.secret_key="secretkey"
app.config['ENV']='development'
app.config['DEBUG']=True
app.config['TESTING']=True

tables = {
  "Users": {"user","password"},
  "Rides": {"username","datetime","source","destination","commuters","rideID"}
}
api=Api(app)


file_handler = FileHandler('errorlog.txt')
file_handler.setLevel(WARNING)

app.logger.addHandler(file_handler)

MONGO_URI = 'mongodb://127.0.0.1:27017/RideShareApp'
client = MongoClient(MONGO_URI)
db = client.RideShareApp


class DBRead(Resource):


    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost'))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='responseQ')
        self.callback_queue = result.method.queue
        
        print('initilization')

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    
    def on_response(self, ch, method, props, body):
        print('here1')
        if self.corr_id == props.correlation_id:
            print('here2')
            self.response = body

   

    def post(self):
        _json = request.json
        table_Name =_json['table']
        Columns = _json['columns']
        getData_clause=''
        if(len(Columns)>0):
            getData_clause=ProcessData(Columns)

        

        print('read query is',getData_clause)
        query_statment='db.'+table_Name+'.'+'find('+getData_clause+')'
        print('read statment is',query_statment)

        
        read_request(self,query_statment)
        evalresponse= eval(query_statment)
       
        documents = ProcessOutputdata(evalresponse,table_Name)
                            
        
        if(len(documents)==0):
            message ={
            'status':204,
            'message':'No data are present'
            }
            resp = jsonify(message)
            resp.status_code=204
            return resp
        else:
            resp = jsonify(documents)
            resp.status_code=201
            return resp

          # = mongo.db.users.find()
        
            
        
class DBWrite(Resource):

    def post(self):
        _json = request.json
        table_Name =_json['table']
        operation_type =_json['typeofoperation']
        Columns = _json['columns']
        insert_data =_json['insertdata']
        
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()

        channel.queue_declare(queue='writeQ')


        if(operation_type=='insert'):
            insert_query1 = ProcessData(insert_data)
            print('insert query is',insert_query1)
            query_statment='db.'+table_Name+'.'+operation_type+'('+insert_query1+')'
            print('query statment is ',query_statment)
            try:
                channel.basic_publish(exchange='',routing_key='writeQ',body=query_statment)
                print(" [x] Sent 'insert query!'")

                resp = jsonify("record inserted successfully")
                resp.status_code=201
                return resp
            except:
                resp = jsonify("some exception occured")
                resp.status_code=400
                return resp
        
        if(operation_type=='update'):
            update_clause=ProcessData(Columns)
            update_statment='db.'+table_Name+'.'+operation_type+'('+update_clause+',{'+'\'$set''\''+':'+ProcessData(insert_data)+'})'
            print('update statment is',update_statment)

            try:
                channel.basic_publish(exchange='',routing_key='writeQ',body=update_statment)
                print(" [x] Sent 'update query!'")
                resp = jsonify("record updated successfully")
                resp.status_code=201
                return resp
            except:
                resp = jsonify("Exception Record updation did not happen")
                resp.status_code=400
                return resp 


        if(operation_type=='delete'):
            delete_clause=ProcessData(Columns)
            delete_statement='db.'+table_Name+'.remove('+delete_clause+')'
            print('delete statment is',delete_statement)

            try:
                channel.basic_publish(exchange='',routing_key='writeQ',body=delete_statement)
                print(" [x] Sent 'delete query!'")
                resp = jsonify("record deleted successfully")
                resp.status_code=201
                return resp
            except:
                resp = jsonify("Exception Record updation did not happen")
                resp.status_code=400
                return resp 

   
    #routing info


class CrashMaster(Resource):
    def get(self):
        message ={
            'status':201,
            'message':'master crashed successfully'
        }
        resp = jsonify(message)
        resp.status_code=201
        return resp


class CrashSlave(Resource):    
    def get(self):
        message ={
            'status':201,
            'message':'slave crashed successfully'
        }
        resp = jsonify(message)
        resp.status_code=201
        return resp


class WorkerList(Resource):
    def get(self):
        message ={
            'status':201,
            'message':'Sorted WorkerList is as follows '
        }
        resp = jsonify(message)
        resp.status_code=201
        return resp


api.add_resource(DBWrite,'/api/v1/db/write')
api.add_resource(DBRead,'/api/v1/db/read')
api.add_resource(CrashMaster,'/api/v1/crash/master')
api.add_resource(CrashSlave,'/api/v1/crash/slave')
api.add_resource(WorkerList,'/api/v1/worker/list')

if __name__ == "__main__":
    app.run(host='0.0.0.0')
