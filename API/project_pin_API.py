import time
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import json
from json import dumps
from kafka import KafkaProducer

app = FastAPI()


class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

KAFKA_TOPIC =  'datapipeline'

producer = KafkaProducer(bootstrap_servers="localhost:29092")


print("Going to be generating order after 3 seconds")
print("Will generate one unique order every 3 seconds")
time.sleep(3)

@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    producer.send(KAFKA_TOPIC, json.dumps(data).encode("utf-8"))
    time.sleep(3)
    return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
