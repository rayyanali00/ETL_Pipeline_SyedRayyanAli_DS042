import pandas as pd
from pymongo import MongoClient

def load_data(df):
    """
    Load final processed data to MongoDB.
    Converts date to datetime to avoid BSON encoding errors.
    """
    df['date_only'] = pd.to_datetime(df['date_only'])
    mongoclient = MongoClient('mongodb+srv://rayyanned_db1:rayyan123@cluster-ned.3frsh.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-NED')
    db = mongoclient['financial_stock']
    collection = db['stocksdata_new']

    records = df.to_dict(orient='records')

    if records:
        collection.insert_many(records)
        print(f"Inserted {len(records)} documents into collection.")
    else:
        print("No records to insert.")