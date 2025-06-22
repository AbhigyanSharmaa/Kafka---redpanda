from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import SchemaRegistryError
from confluent_kafka.schema_registry import Schema

class schemaRegistryClass :
    def __init__(self ,schema_url , subject_name , schema_str , schema_type):
        self.schema_url = schema_url
        self.subject_name = subject_name
        self.schema_str = schema_str
        self.schema_type = schema_type
        self.schema_client = SchemaRegistryClient({"url" : self.schema_url})

    def checkSchema(self):
        try:
            if self.schema_client.get_latest_version(self.subject_name) :
                return True
        except SchemaRegistryError:
                return False
        
    def register_schema(self):
         
         if not self.checkSchema():

            try:
                schema = Schema(self.schema_str , self.schema_type)
                self.schema_client.register_schema(self.subject_name , schema)
                print(f"schema registry is registered successfully")
            except SchemaRegistryError as e:
                print(e)
         else :
             print(f"schema already exists")
            
              

if __name__ == "__main__":

    schema_url = "http://localhost:18081"
    topic_name = "test-topic"
    schema_str = ""
    schema_type = "JSON"

    with open("schema.json") as json_schema:
         schema_str = json_schema.read()
    client = schemaRegistryClass(schema_url , topic_name , schema_str , schema_type)

    client.register_schema()


