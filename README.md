# Pinterest Data Pipeline

## Introduction
The goal of this project is to simulate an **Extract, Transform, Load (ETL) data pipeline** using various **Amazon Web Services (AWS), Databricks, and Apache Kafka**, with the aim of extracting valuable insights from the collected data. The project consists of two main components:

1. **Batch Processing** – Processes data in a 24-hour cycle.
2. **Stream Processing** – Continuously ingests and transforms data in real time.

## Data Model Overview

The dataset models the type of data that Pinterest would likely generate, consisting of millions of records daily. It is structured into three relational tables:

- **User Table (`user`)**  
  Stores user-related information such as first name, last name (concatenated into a `full_name` column during data cleaning), signup date, and age.

- **Pin Table (`pin`)**  
  Contains details about Pinterest posts, including follower count, post category, and the date of the post.

- **Geolocation Table (`geo`)**  
  Holds geographical data such as longitude, latitude, and country.

Since these records are relational, they can be joined on common keys such as an index or a unique user ID. This enables multi-dimensional insights and aggregations, providing a comprehensive view of user behavior, content trends, and geographic distribution.

---

## Batch Processing

### Overview
The batch processing component simulates an ETL process that runs daily and is fully automated.

### Workflow
1. **Data Extraction**
   - `user_posting_emulation_random.py` retrieves **500 random records** from AWS RDS (Relational Database Services) tables (`pin`, `geo`, `user`).
   - Data is sent to **Apache Kafka** topics (`pin`, `geo`, `user`) via an API (Application Program Interface).
   - The records are then stored in an **AWS S3 bucket** in JSON format.

2. **Data Transformation**
   - The JSON files from S3 are loaded into a **Databricks notebook**.
   - The data is converted into **dataframes** and cleaned.
   - SQL queries generate insights, such as:
     - Most popular post categories per country.
     - Median number of followers per signup year.

3. **Automation**
   - AWS **Managed Workflows for Apache Airflow (MWAA)** schedules the tasks.
   - A **Directed Acyclic Graph (DAG)** triggers the Databricks notebook **once per day**.

---

## Stream Processing

### Overview
Unlike batch processing, the **stream processing workflow** runs continuously, transforming and storing data in real-time.

### Workflow
1. **Data Ingestion**
   - `user_posting_emulation_streaming.py` retrieves **500 records** from `user`, `pin`, and `geo` tables.
   - Data is sent to **AWS Kinesis** via an **AWS API Gateway REST API**.

2. **Real-Time Data Processing**
   - Kinesis partitions data into three topics: `user`, `geo`, and `pin`.
   - Databricks reads the streaming data and structures it into **dataframes**.
   - Cleaned data is stored in **Delta tables** within the **Databricks Hive metastore**.

---

## File Structure

### 'db_connector.py'
 Contains the class AWSDBConnector, this class contains two methods that handle the sqlalchemy connection to the AWS relational database.

#### **Key Components**
- **read_db_creds**: Reads the database credentials from a YAML file.
- **create_db_connector**: Takes the credentials read from **read_db_creds** and uses them to create the sqlalchemy engine.


### `user_posting_emulation_random.py`
Simulates user activity and sends extracted data to an AWS EC2 instance running Apache Kafka via **AWS API Gateway**.

#### **Key Components**
- **Database Connection**: Uses `AWSDBConnector` to fetch credentials and connect to AWS RDS MySQL.
- **Data Extraction & Processing**:
  - Extracts **500 random rows** from `user`, `pin`, and `geo` tables.
  - Sends data to **Kafka topics** via API Gateway.
- **Functions**:
  - `send_user_requests(user_result)` → Sends user data.
  - `send_pin_requests(pin_result)` → Sends Pinterest post data.
  - `send_geo_requests(geo_result)` → Sends geolocation data.

### `user_posting_emulation_streaming.py`
Similar to `user_posting_emulation.py`, but sends data to **AWS Kinesis** for real-time processing.

#### **Key Components**
- **Database Connection**: Uses `AWSDBConnector` to fetch credentials and connect to AWS RDS MySQL.
- **Data Extraction & Processing**:
  - Extracts **500 random rows** from `user`, `pin`, and `geo` tables.
  - Sends data to **AWS Kinesis partitions** via **AWS API Gateway**.
- **Functions**:
  - `send_user_requests(user_result)` → Sends user data to Kinesis.
  - `send_pin_requests(pin_result)` → Sends Pinterest post data to Kinesis.
  - `send_geo_requests(geo_result)` → Sends geolocation data to Kinesis.

### `airflow_databricks_dag.py`
An **Apache Airflow DAG** that schedules **Databricks notebook execution**.

#### **Key Components**
- **DAG Configuration (`default_args`)**:
  - Owned by **Robert Edwards**.
  - Retries **once** with a **2-minute delay** on failure.
  - Runs **daily** (`@daily` schedule interval).
- **Databricks Notebook Execution**:
  - Uses **`DatabricksSubmitRunOperator`** to submit a job to Databricks.
  - Executes the notebook at `/Workspace/Users/robbiejedwards@hotmail.com/pinterest_project_databricks`.
  - Runs on cluster `1108-162752-8okw8dgg`.

---

## API Structure

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

---

## ETL Pipeline Architecture 

<img width="1009" alt="Screenshot 2025-01-29 at 17 03 29" src="https://github.com/user-attachments/assets/5faf58c2-c17c-4e36-b5da-e42fcb3fc7ab" />


## Summary
This project successfully simulates an ETL pipeline using AWS, Databricks, and Apache Kafka for Pinterest data processing. It demonstrates both **batch and stream processing**, leveraging cloud services to extract insights in an automated and scalable manner.


## ETL Pipeline Architecture 

<img width="1009" alt="Screenshot 2025-01-29 at 17 03 29" src="https://github.com/user-attachments/assets/5faf58c2-c17c-4e36-b5da-e42fcb3fc7ab" />


## Summary
This project successfully simulates an ETL pipeline using AWS, Databricks, and Apache Kafka for Pinterest data processing. It demonstrates both **batch and stream processing**, leveraging cloud services to extract insights in an automated and scalable manner.