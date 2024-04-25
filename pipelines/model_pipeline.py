from urllib.parse import quote_plus
import yaml
from zenml import pipeline

from steps.model_steps import clean_data, forecast_and_save, read_clean_data

@pipeline(enable_cache=False)
def run_model_pipeline(config):
    # Read clean data
    username = quote_plus(config['mongodb']['user_name'])
    password = quote_plus(config['mongodb']['user_password'])
    uri_start= config['mongodb']['uri_start']
    uri_end= config['mongodb']['uri_end']

    uri = uri_start + username + ':' + password +'@'+ uri_end
    transaction_df = read_clean_data(uri,"Transactions_Database","Clean_Transactions_Data")
    
    if transaction_df is None:
        raise ValueError("Error: Failed to read clean data.")
    
    # Clean the data
    cleaned_df = clean_data(transaction_df)
    
    if cleaned_df is None:
        raise ValueError("Error: Failed to clean data.")

    forecast_and_save(cleaned_df, "Transactions_Database",'transactions_per_day', uri)
    
    # Forecast and save 'CTA'
    forecast_and_save(cleaned_df, "Transactions_Database",'CTA', uri)
