from urllib.parse import quote_plus
from zenml import pipeline
from steps.data_steps import add_time_features, calculate_transactions_per_day, generate_CTA, load_json, remove_columns,save_to_mongoDB

@pipeline(enable_cache=False)
def run_data_pipeline(json_file_path: str,config):
    # Check if JSON file path is provided
    if not json_file_path:
        raise ValueError("Error: JSON file path is not provided.")

    # Load JSON data
    json_data = load_json(json_file_path)

    # Check if JSON data is loaded successfully
    if json_data is None:
        raise ValueError("Error: Failed to load JSON data.")

    # Columns to keep
    columns_to_keep = ['tx', 'timestamp']

    # Remove unnecessary columns
    df = remove_columns(json_data, columns_to_keep)

    # Check if columns are removed successfully
    if df is None:
        raise ValueError("Error: Failed to remove columns.")

    # Add time features
    df = add_time_features(df)

    # Check if time features are added successfully
    if df is None:
        raise ValueError("Error: Failed to add time features.")

    # Generate CTA
    df = generate_CTA(df)

    # Check if CTA is generated successfully
    if df is None:
        raise ValueError("Error: Failed to generate CTA.")

    # Calculate transactions per day
    df = calculate_transactions_per_day(df)

    # Check if transactions per day are calculated successfully
    if df is None:
        raise ValueError("Error: Failed to calculate transactions per day.")

    username = quote_plus(config['mongodb']['user_name'])
    password = quote_plus(config['mongodb']['user_password'])
    uri_start= config['mongodb']['uri_start']
    uri_end= config['mongodb']['uri_end']

    uri = uri_start + username + ':' + password +'@'+ uri_end

    save_to_mongoDB(df,uri,"Transactions_Database","Clean_Transactions_Data")

if __name__=='__main__':
    pass
