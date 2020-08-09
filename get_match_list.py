import requests
import dotenv
import os
from pymongo import MongoClient
import argparse

from core import connect

parser = argparse.ArgumentParser()
parser.add_argument("--type", choices=['new', 'old'], default='new')
args = parser.parse_args()

dotenv.load_dotenv( dotenv.find_dotenv() )

def get_public_matchs( **kwargs ):
    url = 'https://api.opendota.com/api/publicMatches'
    url += "?" + "&".join([ f'{k}={v}' for k,v in kwargs.items() ])
    response = requests.get(url)
    data = response.json()
    return data

def insert_data( data:list, collection_name:str ):
    *_, collection = connect.connect_mongo( database_name='dota', collection_name='matchs')
    count = 0
    for i in data:
        if collection.find( {'match_id': i['match_id'] } ).count():
            count += 1
        else:
            ok = collection.insert_one( i )
    return count    

def get_new_matches():
    data = get_public_matchs()
    count = insert_data( data, collection_name='matchs' )
    while count == 0:
        last_match = min( [ i['match_id'] for i in data ] )
        data = get_public_matchs( less_than_match_id=last_match )
        count = insert_data( data, collection_name='matchs' )

def get_history_matches():
    collection = connect.connect_mongo()[-1]
    last_match = collection.find().sort("match_id", 1).limit(1).next()['match_id']
    while True:
        data = get_public_matchs( less_than_match_id=last_match )
        count = insert_data( data, collection_name='matchs' )
        last_match = min( [ i['match_id'] for i in data ] )

if __name__ == "__main__":
    if args.type == 'old':
        get_history_matches()
    else:
        get_new_matches()