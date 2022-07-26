import configparser
from datetime import datetime
from kafka import KafkaConsumer
import boto3
import json

config = configparser.ConfigParser()
configFilePath = '/Users/barnabymorgan/Desktop/DataPipeline/aws.conf'
config.read(configFilePath)

def upload_json():
    pass

KAFKA_TOPIC = "datapipeline"

consumer = KafkaConsumer(
    KAFKA_TOPIC, 
    bootstrap_servers="localhost:29092"
)

creds_s3 = {
    'bucket_name': "project-data-pipeline",
     'access_key_id': config['read']['accessKeyId'], 
     'secret_access_key': config['read']['secretAccessKey'],
     'access_region': "eu-west-2"
    }
print(creds_s3)
# if __name__ == "main":
#    print("Gonna start listening")

while True:
    for message in consumer:
        print("Here is a message..")
        consumed_message = json.loads(message.value.decode())
        consumed_at = datetime.today().strftime('%Y-%m-%d')
        uuid = consumed_message['unique_id']
        consumed_message = json.dumps(consumed_message)
        print(type(consumed_message))

        client = boto3.client(
            's3',
            aws_access_key_id=creds_s3['access_key_id'],
            aws_secret_access_key=creds_s3['secret_access_key']
        )

        client.put_object(
                        Body=consumed_message,
                        Bucket="project-data-pipeline", 
                        Key=f"{consumed_at}/{uuid}.json"
                        )    
        """
        # upload the local CSV to the S3 bucket
        s3 = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key)
        s3_file = local_filename
        s3.upload_file(local_filename, bucket_name, s3_file)
        """
