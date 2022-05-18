from kafka import KafkaConsumer
import boto3
import json

def upload_json():
    pass

KAFKA_TOPIC = "datapipeline"

consumer = KafkaConsumer(
    KAFKA_TOPIC, 
    bootstrap_servers="localhost:29092"
)

creds_s3 = {
    'bucket_name': "project-data-pipeline",
     'access_key_id': "",
     'secret_access_key': "",
     'access_region': "eu-west-2"
    }

# if __name__ == "main":
#    print("Gonna start listening")

while True:
    for message in consumer:
        print("Here is a message..")
        consumed_message = json.loads(message.value.decode())
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
                        Key=f"{uuid}.json"
                        )    
        """
        parser = configparser.ConfigParser()
        parser.read("pipeline.conf")
        access_key = parser.get("aws_boto_credentials",
                        "access_key")
        secret_key = parser.get("aws_boto_credentials",
                        "secret_key")
        bucket_name = parser.get("aws_boto_credentials",
                        "bucket_name")

        # upload the local CSV to the S3 bucket
        s3 = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key)
        s3_file = local_filename
        s3.upload_file(local_filename, bucket_name, s3_file)
        """
