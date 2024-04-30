import os
import yaml
import pandas as pd
from zenml import pipeline, step
from urllib.parse import quote_plus
from pymongo.mongo_client import MongoClient
from utils import generate_random_transactions_CTA, create_transactions_one_day


@step
def read_data(mongo_uri: str, db_name: str, collection_names: list[str]) -> pd.DataFrame:
    '''The function `read_data` reads data from MongoDB collections into a single pandas DataFrame,
    handling exceptions for connection errors and other issues.

    Parameters
    ----------
    mongo_uri : str
        The URI string for connecting to the MongoDB server.
    db_name : str
        The name of the MongoDB database.
    collection_names : list[str]
        A list of collection names from which data needs to be read.

    Returns
    -------
    pd.DataFrame
        Returns a single pandas DataFrame with concatenated data from the specified MongoDB collections.
        If an error occurs during the process, an empty DataFrame is returned.
    '''
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        
        dfs = []
        for collection_name in collection_names:
            collection = db[collection_name]
            cursor = collection.find()
            df = pd.DataFrame(list(cursor))
            df.drop(columns='_id',inplace=True)
            dfs.append(df)
        
        
        combined_df = pd.concat(dfs, axis=1)
        combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
        combined_df.reset_index(drop=True, inplace=True)
        
        return combined_df
    except Exception as e:
        print(f"An error occurred while reading data from MongoDB: {e}")
        return pd.DataFrame()

@step
def modify_forecasts(df: pd.DataFrame, trx_add: int = 0, trx_mul: int = 1, CTA_add: int = 0, CTA_mul: int = 1) -> pd.DataFrame:
    '''The function `modify_forecasts` takes a DataFrame and modifies two columns by adding and multiplying
    specified values, handling KeyError and other exceptions.
    
    Parameters
    ----------
    df : pd.DataFrame
        The function `modify_forecasts` takes a DataFrame `df` as input along with optional parameters
    `trx_add`, `trx_mul`, `CTA_add`, and `CTA_mul` to modify specific columns in the DataFrame.
    trx_add : int, optional
        The `trx_add` parameter represents the value to add to the 'transactions_per_day_forecast' column
    in the DataFrame. It is used to adjust the forecasted number of transactions per day by adding this
    value.
    trx_mul : int, optional
        The `trx_mul` parameter in the `modify_forecasts` function is used to multiply the existing values
    in the 'transactions_per_day_forecast' column of the DataFrame `df`. This parameter allows you to
    scale the forecasted values by a certain factor.
    CTA_add : int, optional
        CTA_add is a parameter that represents the value to add to the 'CTA_forecast' column in the
    DataFrame during the modification process.
    CTA_mul : int, optional
        The `CTA_mul` parameter in the `modify_forecasts` function is used to multiply the existing
    'CTA_forecast' values in the DataFrame by the specified value. This allows you to scale the CTA
    forecasts by a certain factor.
    
    Returns
    -------
        The function `modify_forecasts` is returning a pandas DataFrame with the modified forecasts for
    'transactions_per_day_forecast' and 'CTA_forecast' columns based on the provided transformation
    parameters `trx_add`, `trx_mul`, `CTA_add`, and `CTA_mul`.
    
    '''
    try:
        df['transactions_per_day_forecast'] = (df['transactions_per_day_forecast'] + trx_add) * trx_mul
        df['CTA_forecast'] = (df['CTA_forecast'] + CTA_add) * CTA_mul
    except KeyError as e:
        print(f"KeyError: {e}")
    except Exception as e:
        print(f"An error occurred while modifying forecasts: {e}")
    return df

@step
def generate_transactions(combined_df:pd.DataFrame)->pd.DataFrame:
    '''The function `generate_transactions` generates random transactions based on forecasts and adds them
    to a DataFrame.
    
    Parameters
    ----------
    combined_df
        Combined_df is a DataFrame containing forecasts for CTA (Call to Action) and transactions per day.
    The function `generate_transactions` takes this DataFrame as input and generates random transactions
    based on the forecasts provided in the DataFrame. It iterates over each row in the DataFrame, using
    the CTA forecast and
    
    Returns
    -------
        The function `generate_transactions` is returning the `combined_df` DataFrame with an additional
    column 'transactions' that contains the generated transactions based on the forecasts.
    
    '''
    """Generate random transactions based on forecasts."""
    transactions = []
    try:
        for idx in range(len(combined_df)):
            transaction = generate_random_transactions_CTA(combined_df['CTA_forecast'][idx],
                                                           combined_df['transactions_per_day_forecast'][idx])
            transactions.append(transaction)
    except Exception as e:
        print(f"An error occurred while generating transactions: {e}")
    combined_df['transactions'] = transactions
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
    return combined_df


@step
def save_transactions_data_to_mongodb(combined_df: pd.DataFrame, mongo_uri: str, db_name: str, collection_name: str) -> None:
    '''The function `save_transactions_data_to_mongodb` saves transaction data from a combined DataFrame to MongoDB.'''
    client = MongoClient(mongo_uri)
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
        return  # Exit if connection fails

    db = client[db_name]
    collection = db[collection_name]

    # Delete previous data from the collection
    try:
        result = collection.delete_many({})
        print(f"Deleted {result.deleted_count} documents from collection '{collection_name}'")
    except Exception as e:
        print(f"An error occurred while deleting previous data: {e}")
        return  # Exit if deletion fails

    # Insert fresh data into the collection
    try:
        for idx, row in combined_df.iterrows():
            date = row['timestamp'].date()
            transaction_lists = row['transactions']
            df = create_transactions_one_day(date, transaction_lists)
            df = df.sort_values("Timestamp").reset_index(drop=True)
            records = df.to_dict(orient='records')
            collection.insert_many(records)
            print(f"Data saved to MongoDB for date {date}")
    except Exception as e:
        print(f"An error occurred while saving data to MongoDB: {e}")


if __name__ == "__main__":
    pass