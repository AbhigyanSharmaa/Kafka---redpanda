from confluent_kafka.admin import AdminClient , NewTopic


class Admin:
    def __init__ (self, bootstrap_server):
        self.bootstrap_server = bootstrap_server
        self.admin = AdminClient({"bootstrap.servers" : self.bootstrap_server})
    
    def topicExists(self, topic):
         all_topics = self.admin.list_topics()

         return topic in all_topics.topics.keys()



    def create_topic(self , topic):
        if not self.topicExists(topic):
            new_topics = NewTopic(topic)
            self.admin.create_topics([new_topics])
            
            print(f"Topic - {topic} has been created")
        else:
            print("Topic - {topic} already exists")
            

    