from confluent_kafka.schema_registry.avro import AvroSerializer
from typing import cast
from confluent_kafka import Producer #Import Producer from Confluent_kafka module
from producer import ProducerClass 
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Admin.Admin_createTopic import Admin #import Admin class from Admin_createTopic
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from schema_registry.schema_client_registry import schemaRegistryClass 
from confluent_kafka.serialization import SerializationContext , MessageField
import uuid



class userDetails:

    def __init__(self , first_name , last_name , age , email , time):
        self.first_name = first_name
        self.last_name = last_name
        self.age = age
        self.email = email
        self.time = time

    def toDict(self):
        return {"first_name" : self.first_name , "last_name" : self.last_name , "age" : self.age , "email" : self.email , "time" : self.time}

    #creating deliver_report function

def delivery_report(err , msg):
    if err is not None:
        print(f"Failed to deliver message for key : {msg.key()}")
    else:
        print(f"successfully delivered the message.\n Key : {str(msg.key())} , topic : {msg.topic()} , partition : {msg.partition()} , offset : {msg.offset()}")


class avroProducerClass(ProducerClass): # create ProducerClass to invoke producer and send messages

    def __init__ (self , bootstrap_server , topic , schema_registry_client , schema_str ): # constructor with bootraps server , topic name , schema_registry , schema_str
        super().__init__(bootstrap_server , topic)
        self.schema_registry_client = schema_registry_client
        self.schema_str = schema_str
        self.value_serializer = AvroSerializer(schema_registry_client , schema_str)
            
    def sendMessage(self , message): # create a function sendMessage to send message
        try:
 
            
            avro_serialized_msg = self.value_serializer(message , SerializationContext(self.topic , MessageField.VALUE))
            self.producer.produce(topic =self.topic , key = str(uuid.uuid4()) , value = avro_serialized_msg , headers = {"correlation_id" : str(uuid.uuid4())} , on_delivery = delivery_report) # use produce method from producer obj and pass topic name and message
            # print(f"message sent : {avro_serialized_msg}")
        except Exception as e:
            print(e)

    def commit(self): # create a commit method to commit all of the messages after sending
        self.producer.flush()  # we use producer.flush() method to commit all of the messages


if __name__ == "__main__":

    bootstrap_server = "localhost:19092" # define the bootstrap server internal address
    topic = "test-topic-2" #define topic name
    schema_url = "http://localhost:18081"
    schema_str = ""
    schema_type = "AVRO"

    with open("../schema_registry/avroSchema.avsc") as avroSchema:
        schema_str = avroSchema.read()
    admin = Admin(bootstrap_server) # create an admin class object and pass bootstrap_server , checkout Admin_createTopic to know more about admin class
    allTopicKeys = [admin.allTopics()  ]
    admin.create_topic(topic) #create topic using admin obj

    #register schema
    schemaClient = schemaRegistryClass(schema_url , topic , schema_str , schema_type )
    schemaClient.register_schema()

    # get schema from schema registry
    schema_str = schemaClient.get_schema_str()

    #producing the message
    produceMsg =  avroProducerClass(bootstrap_server , topic , schemaClient.schema_client , schema_str) # create an obj produceMsg of Producer class and pass bootstrap server and topic details
    # print(f"the schema is : {schemaClient.schema_client.get_latest_version(topic)}")

    
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
