from confluent_kafka import Producer #Import Producer from Confluent_kafka module
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Admin.Admin_createTopic import Admin #import Admin class from Admin_createTopic

class ProducerClass: # create ProducerClass to invoke producer and send messages

    def __init__ (self , bootstrap_server , topic): # constructor with bootraps server and topic name
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.producer = Producer({'bootstrap.servers' :self.bootstrap_server}) #create a producer object by passing bootstrap.servers details in form of dictionary where key is "bootstrap.servers" and value is "bootstrap_server name"

    def sendMessage(self , message): # create a function sendMessage to send message
        try:
            self.producer.produce(self.topic , message) # use produce method from producer obj and pass topic name and message
        except Exception as e:
            print(e)

    def commit(self): # create a commit method to commit all of the messages after sending
        self.producer.flush()  # we use producer.flush() method to commit all of the messages


if __name__ == "__main__":

    bootstrap_server = "localhost:19092" # define the bootstrap server internal address
    topic = "test-topic" #define topic name
    
    admin = Admin(bootstrap_server) # create an admin class object and pass bootstrap_server , checkout Admin_createTopic to know more about admin class
    admin.create_topic(topic) #create topic using admin obj
    produceMsg =  ProducerClass(bootstrap_server , topic) # create an obj produceMsg of Producer class and pass bootstrap server and topic details


    try:
        while True: 
            message = input("Enter your message : ")
            produceMsg.sendMessage(message) # send the message 
    except KeyboardInterrupt:
        pass #Interrupt the console by keyboard to break this code

    produceMsg.commit() # call commit method to flush all of the messages
