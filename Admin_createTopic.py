from confluent_kafka.admin import AdminClient , NewTopic # import adminClient and new topic from confluent_kafka.admin


class Admin:
    def __init__ (self, bootstrap_server): #constructor and pass bootsrap server detail
        self.bootstrap_server = bootstrap_server
        self.admin = AdminClient({"bootstrap.servers" : self.bootstrap_server}) # admin obj of AdminClient to pass bootstrap.servers details in dict format
    
    def topicExists(self, topic): # method to check if a topic exists or not
        all_topics = self.admin.list_topics() # we can get the topics details by using admin.list_topics , we will get a dictionary
        return topic in all_topics.topics.keys() # check if topic exists in all_topics.keys()
    



    def create_topic(self , topic): # method to create a new topic
        if not self.topicExists(topic): # check if topic doesnt exists
            new_topics = NewTopic(topic) # create new topic by using NewTopic class obj
            self.admin.create_topics([new_topics]) # use the new_topics instance in admin.create_topics and pass new_topics as list
            
            print(f"Topic - {topic} has been created") 
        else:
            print(f"Topic - {topic} already exists")
    

            

    