import requests
from time import sleep
import random
import multiprocessing
import boto3
import json
import sqlalchemy
import yaml
from sqlalchemy import text



random.seed(100)


class AWSDBConnector:

    def read_db_creds(self, file_path):
        """
        Reads the database credentials from a YAML file.
        :param file_path: Path to the credentials YAML file.
        :return: A dictionary with the credentials.
        """
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
        return data
        
    def create_db_connector(self, creds):
        host = creds["RDS_HOST"]
        password = creds["RDS_PASSWORD"]
        user = creds["RDS_USER"]
        database = creds["RDS_DATABASE"]
        port = creds["RDS_PORT"]

        engine = sqlalchemy.create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()



def run_random_post_data_loop(db_creds, num_rows=500):
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
            print("  user_result:", user_result)
            print("  geo_result:", geo_result)
            print("  pin_result:", pin_result)

            # Process the fetched results
            if user_result:
                send_user_requests(user_result)
            if geo_result:
                send_geo_requests(geo_result)
            if pin_result:
                send_pin_requests(pin_result)


def send_user_requests(user_result):
    invoke_url = "https://ud7zikav8k.execute-api.us-east-1.amazonaws.com/Test/streams/Kinesis-Prod-Stream/record"
    headers = {'Content-Type': 'application/json'}
    
    # Convert datetime to string for JSON serialization
    user_result["date_joined"] = user_result["date_joined"].isoformat()
    
    # Create the payload matching the API Gateway's mapping template
    payload = json.dumps({
        "StreamName": "Kinesis-Prod-Stream",
        "Data": {
            "index": user_result["ind"],
            "first_name": user_result["first_name"],
            "last_name": user_result["last_name"],
            "age": user_result["age"],
            "date_joined": user_result["date_joined"]
            },  
            "PartitionKey": "user-partition"
    })
           
    print("Payload being sent:", payload)  # Debugging: print payload
    
    try:
        # Send the request using PUT method
        response = requests.put(invoke_url, headers=headers, data=payload)
        
        if response.status_code == 200:
            print("Request successful:", response.json())
            print(response.content)
        else:
            print(f"Failed with status code {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")


def send_pin_requests(pin_result):

    invoke_url = "https://ud7zikav8k.execute-api.us-east-1.amazonaws.com/Test/streams/Kinesis-Prod-Stream/record"
    # Define the headers explicitly
    headers = {'Content-Type': 'application/json'}
    # To send JSON messages you need to follow this structure
    payload = json.dumps({
        "StreamName": "Kinesis-Prod-Stream",
        "Data": {
            # Data should be sent as key-value pairs
            "index": pin_result["index"],
            "unique_id": pin_result["unique_id"],
            "title": pin_result["title"],
            "description": pin_result["description"],
            "poster_name": pin_result["poster_name"],
            "follower_count": pin_result["follower_count"],
            "tag_list": pin_result["tag_list"],
            "is_image_or_video": pin_result["is_image_or_video"],
            "image_src": pin_result["image_src"],
            "downloaded": pin_result["downloaded"],
            "save_location": pin_result["save_location"],
            "category": pin_result["category"]
            },
            "PartitionKey": "pin-partition"
    })
    try:
        response = requests.put(invoke_url, headers=headers, data=payload)
        if response.status_code == 200:
            print("Request successful:", response.json())
        else:
            print(f"Failed with status code {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")

def send_geo_requests(geo_result):

    invoke_url = "https://ud7zikav8k.execute-api.us-east-1.amazonaws.com/Test/streams/Kinesis-Prod-Stream/record"
    # Define the headers explicitly
    headers = {'Content-Type': 'application/json'}
    # Convert datetime to string for JSON serialization
    geo_result["timestamp"] = geo_result["timestamp"].isoformat()  # Convert to ISO 8601 format
    # To send JSON messages you need to follow this structure
    payload = json.dumps({
        "StreamName": "Kinesis-Prod-Stream",
        "Data": {
            "index": geo_result["ind"],
            "timestamp": geo_result["timestamp"],
            "latitude": geo_result["latitude"],
            "longitude": geo_result["longitude"],
            "country": geo_result["country"]
            },
            "PartitionKey": "geo-partition"
    })
    try:
        response = requests.put(invoke_url, headers=headers, data=payload)
        if response.status_code == 200:
            print("Request successful:", response.json())
            print(response.content)
        else:
            print(f"Failed with status code {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")



if __name__ == "__main__":
    # Initialize the connector object
    connection = AWSDBConnector()
    # Load the credentials
    file_path = "db_creds.yaml"
    db_creds = connection.read_db_creds(file_path)

    # Fetch and process 500 rows randomly
    try:
        run_random_post_data_loop(db_creds)
    except Exception as e:
        print(f"An error occurred: {e}")