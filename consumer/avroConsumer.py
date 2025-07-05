from confluent_kafka import Consumer
from confluent_kafka.schema_registry.avro import AvroDeserializer
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from schema_registry.schema_client_registry import schemaRegistryClass 
from confluent_kafka.serialization import SerializationContext , MessageField

class avroConsumerClass :

    def __init__(self , bootstrap_server , topic , group_id , schema_registry_client , schema_str):
        self.bootstrap_server = bootstrap_server
        self.topic = topic
        self.group_id = group_id
        self.schema_registry_client = schema_registry_client
        self.schema_str = schema_str
        self.consumer = Consumer({"bootstrap.servers" : self.bootstrap_server , "group.id" : self.group_id})
        self.value_deserialzer = AvroDeserializer( schema_registry_client , schema_str)

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
                message = msg.value()
                deserialized_message = self.value_deserialzer(message , SerializationContext(self.topic , MessageField.VALUE))
                print(f"Message consumed successfully! \nKey is : {msg.key().decode('utf-8')}" )
                for key , value in deserialized_message.items():
                    print(f"{key} : {value}")

        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

if __name__ == "__main__":
    bootstrap_server = "localhost:19092" # define the bootstrap server internal address
    schema_url = "http://localhost:18081"
    schema_str = ""
    schema_type = "AVRO"
    topic = "test-topic-2" #define topic name
    group_id = "test-group-id"
    schema_registry_client = schemaRegistryClass(schema_url , topic , schema_str , schema_type)
    schema_str = schema_registry_client.get_schema_str()
    consumerClient = avroConsumerClass(bootstrap_server , topic , group_id , schema_registry_client.schema_client , schema_str)
    consumerClient.consume_msg()