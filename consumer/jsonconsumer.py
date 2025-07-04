import json
from confluent_kafka import Consumer

class ConsumerClass :

    def __init__(self , bootstrap_server , topic , group_id):
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.group_id = group_id
        self.consumer = Consumer({"bootstrap.servers" : self.bootstrap_server , "group.id" : self.group_id})

    def consume_msg(self):
        self.consumer.subscribe([self.topic])
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"error while consuming the message : {msg.error()}")
                    continue
                msg_dict = json.loads(msg.value().decode('utf-8'))
                print(f"message consumemd ;")
                for key , value in msg_dict.items():
                    print(f"{key} : {value}" , end="\n")
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

if __name__ == "__main__":
    bootstrap_server = "localhost:19092" # define the bootstrap server internal address
    topic = "test-topic" #define topic name
    group_id = "test-group-id"

    consumerClient = ConsumerClass(bootstrap_server , topic , group_id)
    consumerClient.consume_msg()