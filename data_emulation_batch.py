import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text

from datetime import datetime


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    #invoke URL-1
    invoke_url_pin = "https://kshmcn7hx6.execute-api.us-east-1.amazonaws.com/test/topics/0adad64f7925.pin"

    #invoke URL-2
    invoke_url_geo = "https://kshmcn7hx6.execute-api.us-east-1.amazonaws.com/test/topics/0adad64f7925.geo"

    #invoke URL-3
    invoke_url_user = "https://kshmcn7hx6.execute-api.us-east-1.amazonaws.com/test/topics/0adad64f7925.user"

    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                #print(pin_result)
            
                #json dumps (payload)
                payload = json.dumps({
                            "records": [
                                {
                                #Data should be send as pairs of column_name:value, with different columns separated by commas       
                                #"value": {"index": example_df["index"], "name": example_df["name"], "age": example_df["age"], "role": example_df["role"]}
                                "value": {"index": pin_result["index"],
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
                                        }
                                }
                            ]
                        })
                
                #put request
                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                response = requests.request("POST", invoke_url_pin, headers=headers, data=payload)
                print(response.status_code)


            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                #print(geo_result)

                #json dumps
                payload = json.dumps({
                            "records": [
                                {
                                #Data should be send as pairs of column_name:value, with different columns separated by commas       
                                #"value": {"index": example_df["index"], "name": example_df["name"], "age": example_df["age"], "role": example_df["role"]}
                                "value": {"ind": geo_result["ind"],
                                          "timestamp": str(geo_result["timestamp"]),
                                          "latitude": geo_result["latitude"],
                                          "longitude": geo_result["longitude"],
                                          "country": geo_result["country"]
                                        }
                                }
                            ]
                        })

                #put request
                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                response = requests.request("POST", invoke_url_geo, headers=headers, data=payload)
                print(response.status_code)


            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                #print(user_result)

                payload = json.dumps({
                            "records": [
                                {
                                #Data should be send as pairs of column_name:value, with different columns separated by commas       
                                #"value": {"index": example_df["index"], "name": example_df["name"], "age": example_df["age"], "role": example_df["role"]}
                                "value": {"ind": user_result["ind"],
                                          "first_name": user_result["first_name"],
                                          "last_name": user_result["last_name"],
                                          "age": user_result["age"],
                                          "date_joined": str(user_result["date_joined"])
                                        }
                                }
                            ]
                        })
            
                #put request
                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                response = requests.request("POST", invoke_url_user, headers=headers, data=payload)
                print(response.status_code)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')