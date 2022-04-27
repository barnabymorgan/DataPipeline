# Pinterest Data Processing Pipeline

- Developed an end-to-end data processing pipeline in Python based on Pinterests experiment processing pipeline. 
- Implemented based on Lambda architecture to take advantage of both batch and stream-processing.
- Created an API and used Kafka to distribute the data between S3 for batch processing and Spark streaming for stream-processing.
- Stream processing data was processed using Spark Streaming and saved to a PostgresSQL database for real-time analysis and monitoring. 
- Batch data was extracted from S3 and transformed in Spark using Airflow to orchestrate the transformations. 
- Batch data was then loaded into HBase for long term storage, ad-hoc analysis using Presto and monitored using Prometheus and Grafana.

## Milestone 1: Data Ingestion - Configure the API

- The FastAPI framework allows for fast and easy construction of APIs and is combined with pydantic, which is used to assert the data types of all incoming data to allow for easier processing later on. The server is ran locally using uvicorn, a library for ASGI server implementation.
  
```python
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


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)

```

## Milestone 2: Data Ingestion - Consuming the data in Kafka 

- This milestone covered creating and initialising Kafka topics. This was then setup to have the API sends data to the Kafka topic (producer) with other scripts receiving this data (consumer). 

- Example below:

```bash
/bin/kafka-topics.sh --list --zookeeper 127.0.0.1:2181
```

- The above command is used to check whether the topic has been created successfully, once confirmed the API script is edited to send data to the created kafka topic. The docker container has an attached volume which allows editing of files to persist on the container. The result of this is below:

```python
"""Insert your code here"""
```

## Milestone 3: Batch Processing - Ingest data into the data lake

- Continue this process for every milestone, making sure to display clear understanding of each task and the concepts behind them as well as understanding of the technologies used.

- Also don't forget to include code snippets and screenshots of the system you are building, it gives proof as well as it being an easy way to evidence your experience!

# Milestone 4: Batch Processing - Process the data using Spark

- Continue this process for every milestone, making sure to display clear understanding of each task and the concepts behind them as well as understanding of the technologies used.

- Also don't forget to include code snippets and screenshots of the system you are building, it gives proof as well as it being an easy way to evidence your experience!

# Milestone 5: Batch Processing - Send the data to Apache HBase

- Continue this process for every milestone, making sure to display clear understanding of each task and the concepts behind them as well as understanding of the technologies used.

- Also don't forget to include code snippets and screenshots of the system you are building, it gives proof as well as it being an easy way to evidence your experience!

# Milestone 6: Batch Processing - Set up Presto so you can run ad-hoc queries 

- Continue this process for every milestone, making sure to display clear understanding of each task and the concepts behind them as well as understanding of the technologies used.

- Also don't forget to include code snippets and screenshots of the system you are building, it gives proof as well as it being an easy way to evidence your experience!

# Milestone 8: Batch Processing - Orchestrate the batch processing using Airflow

- Continue this process for every milestone, making sure to display clear understanding of each task and the concepts behind them as well as understanding of the technologies used.

- Also don't forget to include code snippets and screenshots of the system you are building, it gives proof as well as it being an easy way to evidence your experience!

## Conclusions

- Maybe write a conclusion to the project, what you understood about it and also how you would improve it or take it further.

- Read through your documentation, do you understand everything you've written? Is everything clear and cohesive?
