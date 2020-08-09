import requests
import dotenv
import os
from pymongo import MongoClient
import argparse

dotenv.load_dotenv( dotenv.find_dotenv() )

def connect_mongo(database_name='dota', collection_name='matchs'):
    host = os.getenv("MONGO_HOST")
    port = int(os.getenv("MONGO_PORT"))
    client = MongoClient(host, port)
    database = client[database_name]
    collection = database[collection_name]
    return client, database, collection
