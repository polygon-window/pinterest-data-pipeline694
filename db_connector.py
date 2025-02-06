import sqlalchemy
import yaml


class AWSDBConnector:

    def read_db_creds(self, file_path: str):
        """
        Reads the database credentials from a YAML file.

        :param file_path: Path to the credentials YAML file.
        :return: A dictionary with the credentials.
        """
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
        return data
    
    def create_db_connector(self, creds: dict):
        host = creds["RDS_HOST"]
        password = creds["RDS_PASSWORD"]
        user = creds["RDS_USER"]
        database = creds["RDS_DATABASE"]
        port = creds["RDS_PORT"]

        engine = sqlalchemy.create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4")
        return engine


