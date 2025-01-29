# Pinterest Data Pipeline

## Introduction

The goal of this project is to simulate an Extract, Transform, Load (ETL) data pipeline using various Amazon Web Services (AWS), Databricks, and Apache Kafka, with the aim of extracting valuable insights from the collected data. The project consists of two main components: batch processing and stream processing.

## Batch Processing

The objective of the batch processing portion is to simulate an ETL process on Pinterest data with a 24-hour batch cycle. The goal is for the entire ETL process to be fully automated and run daily.

The batch process starts with `user_posting_emulation_random.py`. This script connects to an AWS RDS database and randomly retrieves a predefined number of listings (500 in this case) from the `pin`, `geo`, and `user` tables. The data is then sent to an AWS EC2 instance running Apache Kafka, which ingests the data into three topics (`pin`, `geo`, and `user`) via an API. The data is subsequently stored in an AWS S3 bucket in JSON format.

Next, the JSON data is read from the S3 bucket into a Databricks notebook. The data is transformed into dataframes for further cleaning. Once the data is cleaned, it is queried using SQL within the notebook. These queries provide insights into each batch of data, such as identifying the most popular post categories in each country or calculating the median number of followers for users by signup year.

To automate the batch processing, I used AWS Managed Workflows for Apache Airflow (MWAA) to schedule the tasks. I created a Directed Acyclic Graph (DAG) to trigger the notebook execution once per day.

## Stream Processing

The second part of the project focuses on stream processing. Unlike batch processing, the stream processing workflow does not require MWAA since the process is continuous, and data will be transformed and stored as it is ingested.

For stream processing, a different Python script, `user_posting_emulation_streaming.py`, is used. This script simulates the ingestion of real-time Pinterest data by reading 500 listings from each of the `user`, `pin`, and `geo` tables. The data is sent to AWS Kinesis—a suite of tools for processing and analyzing real-time streaming data—via a REST API POST method created with AWS API Gateway.

The data is distributed across three Kinesis partitions (`user`, `geo`, and `pin`). From there, the data is read in real time into another Databricks notebook. In the notebook, the streaming data is continuously decoded and structured into `user`, `geo`, and `pin` dataframes. After cleaning, the data is saved to Delta tables within the Databricks Hive metastore.

---
## File Structure

### `user_posting_emulation.py`
This script retrieves random records from an **AWS RDS MySQL database** and sends them to an **AWS API Gateway endpoint** for further processing.  

#### **Key Components**
- **`AWSDBConnector`**: Reads database credentials from a YAML file and establishes a connection to MySQL using SQLAlchemy.  
- **`run_random_post_data_loop(db_creds, num_rows=500)`**:  
  - Randomly selects **500** rows from three tables:  
    - `pinterest_data`  
    - `geolocation_data`  
    - `user_data`  
  - Sends the data to different Kafka topics via **AWS API Gateway**.  
- **Data Processing Functions:**  
  - `send_user_requests(user_result)` → Sends user data.  
  - `send_pin_requests(pin_result)` → Sends Pinterest post data.  
  - `send_geo_requests(geo_result)` → Sends geolocation data.  

This script runs in a loop, making API requests after fetching data, with small delays to simulate real-time streaming.

### `user_posting_emulation_kinesis.py`
This script retrieves random records from an **AWS RDS MySQL database** and sends them to an **AWS Kinesis Data Stream** via **API Gateway** for real-time processing.  

#### **Key Components**
- **`AWSDBConnector`**:  
  - Reads database credentials from a YAML file.  
  - Establishes a connection to MySQL using **SQLAlchemy**.  

- **`run_random_post_data_loop(db_creds, num_rows=500)`**:  
  - Randomly selects **500** rows from the following tables:  
    - `pinterest_data`  
    - `geolocation_data`  
    - `user_data`  
  - Sends the data to different **Kinesis partitions** via **AWS API Gateway**.  

- **Data Processing Functions:**  
  - `send_user_requests(user_result)` → Sends user data to Kinesis.  
  - `send_pin_requests(pin_result)` → Sends Pinterest post data to Kinesis.  
  - `send_geo_requests(geo_result)` → Sends geolocation data to Kinesis.  

Each request is sent using **HTTP PUT** to the API Gateway endpoint, structured to match the expected Kinesis payload format. The script introduces small delays to simulate real-time data ingestion.

### `airflow_databricks_dag.py`
This **Apache Airflow DAG** automates the execution of a **Databricks notebook** on a scheduled basis.

#### **Key Components**
- **DAG Configuration (`default_args`)**:  
  - Owned by **Robert Edwards**.  
  - Retries **once** with a **2-minute delay** on failure.  
  - Runs **daily** (`@daily` schedule interval).  

- **Databricks Notebook Execution**:  
  - Uses **`DatabricksSubmitRunOperator`** to submit a run for the notebook at:  
    `/Workspace/Users/robbiejedwards@hotmail.com/pinterest_project_databricks`.  
  - Connects to an **existing Databricks cluster** (`1108-162752-8okw8dgg`).  

This DAG is designed to **automate Pinterest data processing** in Databricks using **Airflow**. 

---
## API Structure
<img width="280" alt="Screenshot 2025-01-29 at 14 35 23" src="https://github.com/user-attachments/assets/c5cf7695-c987-4419-bb78-b26bc46d4da2" />

### Root Endpoint `/`

#### Proxy Route
- `/{proxy+}`
  - **ANY**: Handles any HTTP method


#### Streams Resource
- `/streams`
  - **GET**: Retrieve a list of available streams

#### Specific Stream
- `/streams/{stream-name}`
  - **GET**: Retrieve details of a specific stream

#### Stream Record
- `/streams/{stream-name}/record`
  - **PUT**: Insert or update a single record in the stream

#### Stream Records
- `/streams/{stream-name}/records`
  - **PUT**: Insert or update multiple records in the stream


