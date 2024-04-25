import os
from urllib.parse import quote_plus
import yaml
from zenml import pipeline
from steps.synthetic_data_steps import generate_transactions, modify_forecasts, read_data,save_transactions_data_to_mongodb

@pipeline(enable_cache=False)
def run_synthetic_data_pipeline(config):
    username = quote_plus(config['mongodb']['user_name'])
    password = quote_plus(config['mongodb']['user_password'])
    uri_start= config['mongodb']['uri_start']
    uri_end= config['mongodb']['uri_end']
    uri = uri_start + username + ':' + password +'@'+ uri_end
    print(uri)
    # Read data
    combined_df = read_data(uri, "Transactions_Database", ["transactions_per_day","CTA"])
    print(combined_df)
    if combined_df is None:
        raise ValueError("Error: Failed to read data.")
    
    # Combine and modify forecasts
    combined_df = modify_forecasts(combined_df, 5, 5, 5, 19)
    
    # Generate transactions
    combined_df = generate_transactions(combined_df)
    
    # Save transactions data
    # save_transactions_data(combined_df, output_folder)

    save_transactions_data_to_mongodb(combined_df, uri, "Transactions_Database", "synthetic_Transactions")
