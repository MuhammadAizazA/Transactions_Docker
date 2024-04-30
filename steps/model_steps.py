import pandas as pd
import numpy as np
from prophet import Prophet
from zenml import pipeline, step
from utils import days
import pandas as pd
from pymongo import MongoClient

@step
def read_clean_data(mongo_uri: str, db_name: str, collection_name: str) -> pd.DataFrame:
    '''The function `read_clean_data` reads data from a MongoDB collection into a pandas DataFrame,
    handling exceptions for connection errors and other issues.

    Parameters
    ----------
    mongo_uri : str
        The URI string for connecting to the MongoDB server.
    db_name : str
        The name of the MongoDB database.
    collection_name : str
        The name of the MongoDB collection from which data needs to be read.
    Returns
    -------
    pandas.DataFrame
        Returns a pandas DataFrame containing the data read from the specified MongoDB collection.
        If an error occurs during the process, an empty DataFrame is returned.
    '''
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        # Fetch data from MongoDB collection
        cursor = collection.find()
        df = pd.DataFrame(list(cursor))
        
        # If 'timestamp' is a field, parse it as dates
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        df=df.drop(columns='_id')
        return df
    except Exception as e:
        print(f"An error occurred while reading data from MongoDB: {e}")
        return pd.DataFrame()

@step
def clean_data(df:pd.DataFrame)->pd.DataFrame:
    '''The function `clean_data` renames a column in a DataFrame to 'ds' and removes timezone information
    from the 'ds' column.
    
    Parameters
    ----------
    df : pd.DataFrame
        The `clean_data` function takes a pandas DataFrame `df` as input and performs some data cleaning
    operations on it. The function renames the 'timestamp' column to 'ds' and removes the timezone
    information from the 'ds' column. If an error occurs during the data cleaning process, it
    
    Returns
    -------
        The function `clean_data` is returning a pandas DataFrame after cleaning the data. If an error
    occurs during the data cleaning process, it will catch the specific error (KeyError or any other
    Exception) and return an empty pandas DataFrame.
    
    '''
    try:
        df = df.rename(columns={'timestamp': 'ds'})
        df['ds'] = df['ds'].dt.tz_localize(None)
        return df
    except KeyError as e:
        print(f"KeyError: {e}")
        return pd.DataFrame
    except Exception as e:
        print(f"An error occurred while cleaning the data: {e}")
        return pd.DataFrame

@step
def forecast_and_save(df:pd.DataFrame, db_name:str,feature:str, mongo_uri:str)->None:
    '''The function `forecast_and_save` uses Facebook Prophet to forecast a specified feature in a
    DataFrame and saves the forecasted values to a MongoDB collection.
    
    Parameters
    ----------
    df : pd.DataFrame
        A pandas DataFrame containing the data for forecasting.
    feature : str
        The `feature` parameter in the `forecast_and_save` function represents the specific feature or
        column in the DataFrame `df` that you want to forecast. It could be something like 'sales',
        'revenue', 'temperature', 'demand', etc. The function will use this feature to generate a
    mongo_uri : str
        The URI string for connecting to the MongoDB server.
    
    '''
    try:
        subdf = df[['ds', feature]].rename(columns={feature: 'y'})
        m = Prophet()
        m.fit(subdf)
        future = m.make_future_dataframe(periods=days, freq='D')
        forecast = m.predict(future)

        if feature == 'transactions_per_day':
            forecast['yhat'] = forecast['yhat'].apply(np.ceil)
            forecast.loc[forecast['yhat'] < 0, 'yhat'] = np.abs(forecast.loc[forecast['yhat'] < 0, 'yhat'])
        elif feature == 'CTA':
            forecast['yhat'] = (forecast['yhat'] / 10e6).astype(int)

        # Save forecasted data to MongoDB
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[feature]
        try:
            result = collection.delete_many({})
            print(f"Deleted {result.deleted_count} documents from collection '{feature}'")
        except Exception as e:
            print(f"An error occurred while deleting previous data: {e}")
            return  # Exit if deletion fails
        records = forecast[['ds', 'yhat']].tail(days).rename(columns={'ds': 'timestamp', 'yhat': feature+'_forecast'}).to_dict(orient='records')
        collection.insert_many(records)
        print(f"Forecasted data for '{feature}' saved to MongoDB")
    except Exception as e:
        print(f"An error occurred while forecasting and saving data for '{feature}': {e}")



if __name__ == "__main__":
    pass
