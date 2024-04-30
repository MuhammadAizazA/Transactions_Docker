import os
import csv
import yaml
import json
import logging
from bson import ObjectId
from fastapi import FastAPI
from datetime import datetime
from pymongo import MongoClient
from urllib.parse import quote_plus
from motor.motor_asyncio import AsyncIOMotorClient
from pipelines.data_pipeline import run_data_pipeline
from pipelines.model_pipeline import run_model_pipeline
from pipelines.synthetic_data_pipeline import run_synthetic_data_pipeline

with open("config.yaml", "r") as yamlfile:
    config = yaml.safe_load(yamlfile)

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Site is Working---Api's---You can Call---make_transactions---read_transactions_from_mongodb"}

@app.get("/make_transactions")
async def transaction_maker():
    main()
    return {"message": "Transactions Created"}


class ObjectIdEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)

@app.get("/read_transactions_from_mongodb")
async def read_transactions_from_mongodb_endpoint():
    # Connect to MongoDB
    username = quote_plus(config['mongodb']['user_name'])
    password = quote_plus(config['mongodb']['user_password'])
    uri_start= config['mongodb']['uri_start']
    uri_end= config['mongodb']['uri_end']

    uri = uri_start + username + ':' + password +'@'+ uri_end

    client = MongoClient(uri)
    db = client[config['mongodb']['database_name']]
    collection = db[config['mongodb']['collection_name']]

    # Read transactions from MongoDB
    transactions = []
    for doc in collection.find():
        transactions.append(doc)

    # Return the transactions as JSON using the custom encoder
    return json.loads(json.dumps(transactions, cls=ObjectIdEncoder))

def load_config(config_file):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    return config

def main():
    # Load configuration
    config = load_config('config.yaml')
    
    # Define file paths from configuration
    raw_data_file = config['paths']['raw_data_file']

    # Run the ZenML pipelines
    try:
        run_data_pipeline(raw_data_file,config)
    except Exception as e:
        logging.error(f"Error executing data pipeline: {e}")

    try:
        run_model_pipeline(config)
    except Exception as e:
        logging.error(f"Error executing model pipeline: {e}")
    try:
        run_synthetic_data_pipeline(config)
    except Exception as e:
        logging.error(f"Error executing synthetic data pipeline: {e}")

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(filename='pipeline.log', level=logging.ERROR, format='%(asctime)s - %(message)s')