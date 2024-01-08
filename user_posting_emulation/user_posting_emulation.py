import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import Engine, text

import yaml
import pathlib
import os

from utils import json_serializer

random.seed(100)


class DataEmulator:
    def __init__(self, credentials_emulation_filepath, credentials_api_filepath):
        '''
        Initialise DataEmulator class and load class variables
        '''
        # Get credentials to emulation database
        self.credentials_emulation = self.read_db_creds(credentials_emulation_filepath)

        # Initialise emulation database engine
        self.engine = self.create_db_connector()
        
        # Invoke URL for Kafka API
        self.credentials_api = self.read_db_creds(credentials_api_filepath)
    
    def read_db_creds(self, filepath: str) -> dict:
        '''
        Read the credentials yaml file and return a dictionary of the credentials.

        Parameters:
        ----------
        filepath: str
            Path to the credentials file
            
        Returns:
        -------
        credentials: dict
            Dictionary containing the credentials
        '''
        with open(filepath, 'r') as file:
            credentials = yaml.safe_load(file)

        return credentials
    
    def create_db_connector(self) -> Engine:
        '''
        Read the credentials from the input filepath. Then, initialise and return 
        an sqlalchemy database engine.

        Returns:
        -------
        engine: Engine
            PostgreSQL database engine
        '''
        # Define the database credentials
        host=self.credentials_emulation['RDS_HOST']
        port=self.credentials_emulation['RDS_PORT']
        database=self.credentials_emulation['RDS_DATABASE']
        user=self.credentials_emulation['RDS_USER']
        password=self.credentials_emulation['RDS_PASSWORD']

        # Try to create engine
        try: 
            engine = sqlalchemy.create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4")
            print(f"Connection to the '{host}' for user '{user}' created successfully.")
            print("---")
            return engine

        except Exception as ex:
            print("Connection could not be made due to the following error: \n", ex)
            print("---")


    def get_emulation_data(self) -> dict:
        '''
        Retrieve data from a random row from the emulation database.

        Returns:
        -------
        result: dict
            Random emulation data in dict format. Keys: "pin", "geo", "user".
        '''
        random_row = random.randint(0, 11000)

        with self.engine.connect() as connection:
            try:
                # Get pin data
                pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
                pin_selected_row = connection.execute(pin_string)
                    
                for row in pin_selected_row:
                    pin_result = dict(row._mapping)

                # Get geolocation data
                geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
                geo_selected_row = connection.execute(geo_string)
                
                for row in geo_selected_row:
                    geo_result = dict(row._mapping)

                # Get user data
                user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
                user_selected_row = connection.execute(user_string)
                    
                for row in user_selected_row:
                    user_result = dict(row._mapping)
                
                # Construct dictionary
                result = {"pin": pin_result, "geo": geo_result, "user": user_result}
                return result
            
            except Exception as e:
                print(f"Error when retrieving emulation data.\nExit with error: {e}")
                return None
    
    def print_to_console(self) -> None:   
        '''
        Run a single emulation data query and print the emulated data.
        ''' 
        # Get data
        emulation_data = self.get_emulation_data()
            
        # Print data
        print("Pin data:")     
        print(emulation_data["pin"])
            
        print("\nGeolocation data:")     
        print(emulation_data["geo"])
            
        print("\nUser data:")     
        print(emulation_data["user"])
            
        print("---")
        
    def print_to_console_cycle(self) -> None:   
        '''
        Run an endless emulation data query cycle and print the emulated data.
        ''' 
        while True:
            sleep(random.randrange(0, 2))   # Add a random delay between data
            self.print_to_console()         # Print emulated user post data

    def post(self, data: dict) -> text:   
        '''
        Post input data to Kafka topics.
        
        Parameters:
        ----------
        data: dict
            Data payload for API post request. Keys: "pin", "geo", "user"
        '''
        valid_keys = ["pin", "geo", "user"]
        
        for key in valid_keys:
            payload = json.dumps({"records": [{"value": data[key]}]}, default=json_serializer)
            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            invoke_url = self.credentials_api["INVOKE_URL_BASE"] + "." + key
                
            print(payload)
            print(invoke_url)

            response = requests.request("POST", url=invoke_url, headers=headers, data=payload)
            return response
        
    def post_emulation(self) -> None:
        '''
        Retrieve emulated data and post to Kafka.
        '''        
        # Get data
        emulation_data = self.get_emulation_data()
        
        # Post data
        print("Posting emulated data to Kafka topic...")
        response = self.post(emulation_data)        
        print(f"Response: {response.status_code}")
        

if __name__ == "__main__":
    # Initialise user posting emulation class
    directory = os.path.dirname(os.path.abspath(__file__))
    creds_emulation_filepath = directory + "/" + "db_creds_aws_emulation.yaml"
    creds_api_filepath = directory + "/" + "db_creds_api.yaml"
    
    emulator = DataEmulator(creds_emulation_filepath, creds_api_filepath)
    
    # Print data
    # emulator.print_to_console_cycle()
    
    # Post data
    emulator.post_emulation()
    