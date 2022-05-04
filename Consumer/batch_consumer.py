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

print("Gonna start listening")

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
{
   "index":7528,
   "unique_id":"fbe53c66-3442-4773-b19e-d3ec6f54dddf",
   "title":"No Title Data Available",
   "description":"No description available Story format",
   "poster_name":"User Info Error",
   "follower_count":"User Info Error",
   "tag_list":"N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
   "is_image_or_video":"multi-video(story page format)",
   "image_src":"Image src error.",
   "downloaded":0,
   "save_location":"Local save in /data/mens-fashion",
   "category":"mens-fashion"
}
{
    "category": "diy-and-crafts",
    "index": 2863,
    "unique_id": "9bf39437-42a6-4f02-99a0-9a0383d8cd70",
    "title": "25 Super Fun Summer Crafts for Kids - Of Life and Lisa",
    "description": "Keep the kids busy this summer with these easy diy crafts and projects. Creative and\u2026",
    "follower_count": "124k",
    "tag_list": "Summer Crafts For Kids,Fun Crafts For Kids,Summer Kids,Toddler Crafts,Crafts To Do,Diy For Kids,Summer Snow,Diys For Summer,Craft Ideas For Girls",
    "is_image_or_video": "image",
    "image_src": "https://i.pinimg.com/originals/b3/bc/e2/b3bce2964e8c8975387b39660eed5f16.jpg",
    "downloaded": 1,
    "save_location": "Local save in /data/diy-and-crafts"
}
"""