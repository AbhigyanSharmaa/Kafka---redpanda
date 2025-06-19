from jsonschema import validate
import json
from typing import cast
from confluent_kafka import Producer #Import Producer from Confluent_kafka module
from producer import ProducerClass 
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Admin.Admin_createTopic import Admin #import Admin class from Admin_createTopic
from datetime import datetime

#defining schema
schema = {
    "type" : "object" , 
    "properties" : {
        "first_name" : {
            "type" : "string"
        } , 
        "last_name" : {"type" : "string"},
        "age" : {
            "type" : "integer" , 
            "minimum" : 0,
        } , 
        "email" : {"type" : "string"} , 
        "time" : {"type" : "string"} ,


    }


    }

class userDetails:

    def __init__(self , first_name , last_name , age , email , time):
        self.first_name = first_name
        self.last_name = last_name
        self.age = age
        self.email = email
        self.time = time

    def toDict(self):
        return {"firstname" : self.first_name , "lastname" : self.last_name , "age" : self.age , "email" : self.email , "time" : self.time}

            

class JsonProducerClass(ProducerClass): # create ProducerClass to invoke producer and send messages

    def __init__ (self , bootstrap_server , topic , schema ): # constructor with bootraps server and topic name
        super().__init__(bootstrap_server , topic)
        self.schema = schema
        self.value_serializer = lambda x : json.dumps(x).encode('utf-8')
            
    def sendMessage(self , message): # create a function sendMessage to send message
        try:
 
            validate(message , self.schema) #validating the schema
            json_msg = self.value_serializer(message)
            self.producer.produce(self.topic , json_msg) # use produce method from producer obj and pass topic name and message
            print("message sent")
        except Exception as e:
            print(e)

    def commit(self): # create a commit method to commit all of the messages after sending
        self.producer.flush()  # we use producer.flush() method to commit all of the messages


if __name__ == "__main__":

    bootstrap_server = "localhost:19092" # define the bootstrap server internal address
    topic = "test-topic" #define topic name
        
    admin = Admin(bootstrap_server) # create an admin class object and pass bootstrap_server , checkout Admin_createTopic to know more about admin class
    allTopicKeys = [admin.allTopics()  ]
    admin.create_topic(topic) #create topic using admin obj
    produceMsg =  JsonProducerClass(bootstrap_server , topic , schema) # create an obj produceMsg of Producer class and pass bootstrap server and topic details


    try:
        while True: 
            first_name = input("Enter your first name : ")
            last_name = input("Enter your last name : ")
            age = int(input("Enter your age : "))
            email = input("Enter your email: ")
            time = datetime.now().isoformat()

            user = userDetails(first_name=first_name , last_name= last_name , age= age , email= email , time=  time)

            message = user.toDict()

            produceMsg.sendMessage(message) # send the message 
    except KeyboardInterrupt:
        pass #Interrupt the console by keyboard to break this code

    produceMsg.commit() # call commit method to flush all of the messages
