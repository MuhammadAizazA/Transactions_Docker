import json
import pandas as pd
import numpy as np
from pymongo import MongoClient
from zenml import step,pipeline
import logging
import re

# set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

@step
def load_json(file_path:str)->pd.DataFrame:
    '''The function `load_json` reads a JSON file into a pandas DataFrame and converts a specific column to
    JSON strings.
    
    Parameters
    ----------
    file_path : str
        The `file_path` parameter in the `load_json` function is a string that represents the path to the
    JSON file from which data needs to be loaded into a Pandas DataFrame.
    
    Returns
    -------
        A pandas DataFrame containing the data loaded from the specified JSON file is being returned.
    
    '''
    logger.info(f"Loading data from {file_path}")
    df = pd.read_json("Data/Raw_Data/transactions.json")
    df['tx'] = df['tx'].apply(lambda x: json.dumps(x))
    return df

@step
def save_to_mongoDB(df: pd.DataFrame, mongo_uri: str, db_name: str, collection_name: str)-> None:

    client = MongoClient(mongo_uri)
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)
    # Check if the database exists
    if db_name not in client.list_database_names():
        print(f"Database '{db_name}' does not exist. It will be created when data is inserted.")

    db = client[db_name]

    # Check if the collection exists
    if collection_name not in db.list_collection_names():
        print(f"Collection '{collection_name}' does not exist. It will be created when data is inserted.")

    collection = db[collection_name]

    try:
        records = df.to_dict(orient='records')
        collection.insert_many(records)
        print(f"Data saved to MongoDB")
    except Exception as e:
        print(f"An error occurred while saving data to MongoDB: {e}")



def extract_amount(tx: str) -> int:
    '''The function `extract_amount` extracts and returns the amount value from a transaction string in
    JSON format.
    
    Parameters
    ----------
    tx : str
        The `tx` parameter is a string that represents a transaction. The function `extract_amount` uses a
    regular expression to search for the amount value within the transaction string and returns it as an
    integer. If the amount is found, it is returned as an integer. If not found, the function returns
    
    Returns
    -------
        The function `extract_amount` returns an integer value representing the amount extracted from the
    input transaction string `tx`. If a match is found for the pattern `"amount": "(\d+)"`, it returns
    the extracted amount as an integer. If no match is found, it returns `None`.
    
    '''
    match = re.search(r'"amount": "(\d+)"', tx)
    if match:
        return int(match.group(1))
    else:
        return None

@step
def remove_columns(df: pd.DataFrame, columns_to_keep: list[str]) -> pd.DataFrame:
    '''This function removes specified columns from a DataFrame, extracts amounts from a 'tx' column, fills
    missing values with the mode, converts amounts to integers, and then drops the 'tx' column before
    returning the modified DataFrame.
    
    Parameters
    ----------
    df : pd.DataFrame
        A pandas DataFrame containing transaction data with a column named 'tx' that needs to be processed.
    columns_to_keep : list[str]
        The `columns_to_keep` parameter in the `remove_columns` function is a list of column names that you
    want to keep in the DataFrame `df` after removing all other columns.
    
    Returns
    -------
        The function `remove_columns` is returning a Pandas DataFrame after performing certain operations
    on it. If an error occurs during the process, it will return `None`.
    
    '''
    try:
        df = df[columns_to_keep]
        amounts = df['tx'].apply(extract_amount)
        amounts = amounts.fillna(amounts.mode()[0])
        amounts = amounts.astype(int)
        df.loc[:, 'amount'] = amounts
        df.drop(columns='tx',inplace=True)
        return df
    except KeyError as e:
        logger.error(f"KeyError: {e}")
        return None
    except Exception as e:
        logger.error(f"An error occurred while removing columns: {e}")
        return None

@step
def add_time_features(df:pd.DataFrame)->pd.DataFrame:
    '''This function adds time-related features such as day/night, weekend, and season based on the
    timestamp column in a DataFrame.
    
    Parameters
    ----------
    df : pd.DataFrame
        The function `add_time_features` takes a pandas DataFrame `df` as input and adds several
    time-related features to it. These features include 'day_night' to determine if it's day or night
    based on the hour of the timestamp, 'weekend' to indicate if it's a weekend
    
    Returns
    -------
        The function `add_time_features` is returning a pandas DataFrame with additional columns for
    day_night, weekend, and season based on the timestamp column in the input DataFrame. If an error
    occurs during the process, it will print an error message and return None.
    
    '''
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['day_night'] = df['timestamp'].dt.hour.apply(lambda x: 'day' if 6 <= x < 18 else 'night')
        df['weekend'] = df['timestamp'].dt.weekday.apply(lambda x: 'yes' if x >= 5 else 'no')
        df['season'] = df['timestamp'].dt.month.apply(lambda x: 'Spring' if 3 <= x <= 5 else ('Summer' if 6 <= x <= 8 else ('Autumn' if 9 <= x <= 11 else 'Winter')))
        return df
    except Exception as e:
        print(f"An error occurred while adding time features: {e}")
        return None

@step
def generate_CTA(df:pd.DataFrame)->pd.DataFrame:
    '''The function `generate_CTA` calculates the mean and standard deviation of non-zero amounts in a
    DataFrame, generates random amounts for zero values, and creates a new column 'CTA' with normalized
    amounts.
    
    Parameters
    ----------
    df : pd.DataFrame
        It seems like you have not provided the DataFrame `df` that is required as input for the
    `generate_CTA` function. Please provide the DataFrame `df` so that I can assist you further with
    generating the CTA column based on the given logic in the function.
    
    Returns
    -------
        The function `generate_CTA` is returning a pandas DataFrame with a new column 'CTA' added, which
    represents the 'amount' column divided by 10e6. If an error occurs during the process, it will print
    an error message and return None.
    
    '''
    try:
        non_zero_amount_mean = df.loc[df['amount'] > 10, 'amount'].mean()
        non_zero_amount_std = df.loc[df['amount'] > 10, 'amount'].std()
        zero_amount_indices = df['amount'] <= 10
        num_zero_values = zero_amount_indices.sum()
        random_amounts = np.abs(np.random.normal(non_zero_amount_mean, non_zero_amount_std, num_zero_values))
        df.loc[zero_amount_indices, 'amount'] = (random_amounts/10e6).astype(int)
        df['CTA'] = df['amount'] / 10e6
        return df
    except Exception as e:
        print(f"An error occurred while generating CTA column: {e}")
        return None

@step
def calculate_transactions_per_day(df:pd.DataFrame)->pd.DataFrame:
    '''The function calculates the number of transactions per day based on a timestamp column in a
    DataFrame.
    
    Parameters
    ----------
    df : pd.DataFrame
        The function `calculate_transactions_per_day` takes a pandas DataFrame `df` as input. The DataFrame
    is expected to have a column named 'timestamp' containing datetime values. The function calculates
    the number of transactions per day by grouping the data based on the date part of the timestamp and
    then counting the number
    
    Returns
    -------
        The function `calculate_transactions_per_day` returns a pandas DataFrame with an additional column
    'transactions_per_day' that represents the count of transactions per day. If an error occurs during
    the calculation process, it will return None and print an error message indicating the issue.
    
    '''
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        df['transactions_per_day'] = df.groupby('date')['timestamp'].transform('count')
        df.drop(columns=['date'], inplace=True)
        return df
    except Exception as e:
        print(f"An error occurred while calculating transactions per day: {e}")
        return None

if __name__=='__main__':
    pass
