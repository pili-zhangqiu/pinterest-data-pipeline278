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

random.seed(100)


class DataEmulator:
    def __init__(self, credentials_filepath: str) -> None:
        '''
        Initialise DataEmulator class and load class variables
        '''
        # Get credentials
        self.credentials_filepath = credentials_filepath
        self.credentials = self.read_db_creds()

        # Initialise engine
        self.engine = self.create_db_connector()
    
    def read_db_creds(self) -> dict:
        '''
        Read the credentials yaml file and return a dictionary of the credentials.

        Returns:
        -------
        credentials: dict
            Dictionary containing the credentials
        '''
        with open(self.credentials_filepath, 'r') as file:
            credentials = yaml.safe_load(file)

        return credentials
        
    '''def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine'''
    
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
        host=self.credentials['RDS_HOST']
        port=self.credentials['RDS_PORT']
        database=self.credentials['RDS_DATABASE']
        user=self.credentials['RDS_USER']
        password=self.credentials['RDS_PASSWORD']

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
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)

        with self.engine.connect() as connection:
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
    
    def print_to_console(self) -> None:   
        '''
        Run an emulation data query and print the emulated data.
        ''' 
        # Get data
        emulation_row = self.get_emulation_data()
        
        # Print data
        print("Pin data:")     
        print(emulation_row["pin"])
        
        print("\nGeolocation data:")     
        print(emulation_row["geo"])
        
        print("\nUser data:")     
        print(emulation_row["user"])
        
        print("---")


if __name__ == "__main__":
    # Initialise database connector to retrieve emulation data
    directory = os.path.dirname(os.path.abspath(__file__))
    credentials_filepath = directory + "/" + "db_creds_aws_emulation.yaml"
    emulator = DataEmulator(credentials_filepath)

    # Print emulation to console
    while True:
        emulator.print_to_console()
    