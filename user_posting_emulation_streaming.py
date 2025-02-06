import requests
from time import sleep
import random
import multiprocessing
import boto3
import json
import sqlalchemy
import yaml
import db_connector
import configparser
from sqlalchemy import text

random.seed(100)

# Retrieve config information from config.ini
config = configparser.ConfigParser()
config.read("config.ini")
# Base URL for API Gateway
INVOKE_URL = config.get("endpoints", "streaming_api_url")


new_connector = db_connector.AWSDBConnector()
# Define headers
HEADERS = {'Content-Type': 'application/json'}

def run_random_post_data_loop(db_creds: dict, num_rows=500):
    """
    Randomly fetches rows from user_data, geolocation_data, and pinterest_data tables
    and processes them.

    :param db_creds: Database credentials.
    :param num_rows: Number of random rows to fetch and process (default is 500).
    :return: None
    """
    for _ in range(num_rows):  
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector(db_creds)
        
        with engine.connect() as connection:
            # Fetch one row from pinterest_data
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            # Fetch one row from geolocation_data
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            # Fetch one row from user_data
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)

            # Print results for debugging
            print("Randomly fetched rows:")
            print(" user_result:", user_result)
            print(" pin_result:", pin_result)
            print(" geo_result:", geo_result)

            # Process the fetched results
            if user_result:
                send_requests(user_result, "user")
            if pin_result:
                send_requests(pin_result, "pin")
            if geo_result:
                send_requests(geo_result, "geo")

def send_requests(result: dict, request_type: str) -> None:
    """
    Sends data to the appropriate Kafka topic via AWS API Gateway.

    :param result: Dictionary containing the data to be sent.
    :param request_type: Type of request ('user', 'pin', 'geo').
    """
    # Define the appropriate endpoint for the request type
    if request_type == "user":
        # Convert datetime to string for JSON serialization
        result["date_joined"] = result["date_joined"].isoformat()
        payload = json.dumps({
            "StreamName": "Kinesis-Prod-Stream",
            "Data": {
                "index": result["ind"],
                "first_name": result["first_name"],
                "last_name": result["last_name"],
                "age": result["age"],
                "date_joined": result["date_joined"]
                },  
                "PartitionKey": "user-partition"
        })
    elif request_type == "geo":
        result["timestamp"] = result["timestamp"].isoformat()  # Convert to ISO 8601 format
        payload = json.dumps({
            "StreamName": "Kinesis-Prod-Stream",
            "Data": {
                "index": result["ind"],
                "timestamp": result["timestamp"],
                "latitude": result["latitude"],
                "longitude": result["longitude"],
                "country": result["country"]
                },
                "PartitionKey": "geo-partition"
        })
    elif request_type == "pin":
        payload = json.dumps({
            "StreamName": "Kinesis-Prod-Stream",
            "Data": {
                # Data should be sent as key-value pairs
                "index": result["index"],
                "unique_id": result["unique_id"],
                "title": result["title"],
                "description": result["description"],
                "poster_name": result["poster_name"],
                "follower_count": result["follower_count"],
                "tag_list": result["tag_list"],
                "is_image_or_video": result["is_image_or_video"],
                "image_src": result["image_src"],
                "downloaded": result["downloaded"],
                "save_location": result["save_location"],
                "category": result["category"]
                },
                "PartitionKey": "pin-partition"
        })
    try:
        response = requests.put(INVOKE_URL, headers=HEADERS, data=payload)
        if response.status_code == 200:
            print("Request succesful:", response.json())
        else:
            print(f"Request failed with status code: {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    
if __name__ == "__main__":
    # Initialize the connector object
    connection = db_connector.AWSDBConnector()
    # Load the credentials
    file_path = "db_creds.yaml"
    db_creds = connection.read_db_creds(file_path)

    # Fetch and process 500 rows randomly
    try:
        run_random_post_data_loop(db_creds)
    except Exception as e:
        print(f"An error occurred: {e}")