# pinterest-data-pipeline694

---
## Introduction

The aim of this project is to simulate an Extract Transform Load (ETL) data pipeline using various Amazon web services (AWS), databricks and Apache Kafka with the goal of gaining new insights from the data collected. There will be two main facets to this project: batch processing and stream processing. 

---
## Batch Processing

The aim of the batch proccesing part of the project is to be able to simulate an ETL process on pinterest data on a 24 hour batch cycle. The intention is for entire ETL process to be completely automated and run daily.

The batch process begins with user_posting_emulation_random.py. This file connects to an AWS RDS database and randomly retrieves a predefined amount of listings (in this case 500) from each of the pin, geo and user tables. This data is then sent to an AWS EC2 instance running Apache Kafta, which ingests the data into 3 topics (pin, geo and user) via an API, which in turn sends the data to an AWS S3 bucket where it is stored in JSON format. The next step of the batch processing involves reading this JSON data from the S3 bucket into a Databricks notebook, from here the data is transformed into dataframes where it can be cleaned.

Once the data has been cleaned the data is queried using SQL within the notebook, these queries will provide various insights on each batch of data including which category of post is most popular in each country or the median count of users followers by sign up year.

The next step was to utilise AWS Managed Workflows for Apache Airflow (MWAA) in order to schedule and automate each batch of processing. I created a Directed Acyclic Graph (DAG) that will trigger the previously mentioned notebook once per day.

---
## Stream Processing

The second part of the project was for stream processing of data, this time MWAA was not neccesary as the the whole process is continuous and data will be transformed and stored as it is ingested. 

A different python file is used for this named user_posting_emulation_streaming.py, similar to the file used in the batch process this reads 500 listings from each table (user, pin, geo) to simulate the ingestion of real time pinterest data. However this file then sends the data to AWS Kinesis, an array of tools for processing and analysing real time streaming data, via an REST API post method I created using AWS API Gateway. The data is sent to three kinesis partitions (user, geo and pin) and from there this data is read in realtime to another Databricks Notebook. In this notebook the streaming data is continuously decoded and structured into user, geo and pin dataframes, before being cleaned and saved to delta tables within Databricks Hive metastore.

---
## File Structure



---
## API Structure
<img width="280" alt="Screenshot 2025-01-29 at 14 35 23" src="https://github.com/user-attachments/assets/c5cf7695-c987-4419-bb78-b26bc46d4da2" />

---
### Insights

