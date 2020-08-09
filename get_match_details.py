import requests
import dotenv
import os
from pymongo import MongoClient
import argparse
import datetime
from tqdm import tqdm
import time

from core import connect

parser = argparse.ArgumentParser()
parser.add_argument("--date_start", "-s", help="Data de inicio da coleta", default="2000-01-01")
parser.add_argument("--date_end", "-e", help="Data de fim da coleta", default="2100-01-01")
args = parser.parse_args()

def get_match_details( match_id, **kwargs ):
    url = f"https://api.opendota.com/api/matches/{match_id}"
    url += "?" + "&".join([ f'{k}={v}' for k,v in kwargs.items() ])
    response = requests.get(url)
    data = response.json()
    return data

def insert_data(data:list):
    *_, collection = connect.connect_mongo( database_name='dota', collection_name='matches_details')
    if collection.find({'match_id': data['match_id']}).count():
        remove = collection.delete_one({'match_id': data['match_id']})
    ok = collection.insert_one( data )
    return None

def get_auto_details( date_start:str, date_end:str ):
    timestamp_start = parse_str_timestamp(date_start)
    timestamp_end = parse_str_timestamp(date_end)
    collections_matches = connect.connect_mongo(collection_name="matchs")[-1]
    collections_details = connect.connect_mongo(collection_name="matches_details")[-1]

    matchs_collected = [i['match_id'] for i in collections_details.find({},{"match_id":1})]

    matchs_in_time = collections_matches.find( { "$and":
                                                    [{"start_time":{"$gt":timestamp_start, "$lt":timestamp_end}},
                                                     {"match_id": {"$nin": matchs_collected} }]},
                                               {"match_id":1} )
    
    match_ids = [ i['match_id'] for i in matchs_in_time ]

    for i in tqdm(match_ids):
        data = get_match_details(i)
        try:
            insert_data(data)
        except KeyError:
            while 'error' in data:
                time.sleep(60)
                data = get_match_details(i)
            insert_data(data)

def parse_str_timestamp(date:str):
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    return int(date.replace(tzinfo=datetime.timezone.utc).timestamp())

if __name__ == "__main__":
    get_auto_details(args.date_start, args.date_end)
