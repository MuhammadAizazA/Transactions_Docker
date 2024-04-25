import datetime
import random
import pandas as pd
import yaml

def load_config(config_file):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    return config

config = load_config('config.yaml')
days = config['utils']['days']

def generate_random_transactions_CTA(target_number, divisor):
    transactions = []

    try:
        current_value = target_number

        while divisor > 2:
            result = current_value / divisor

            # Generating a random number within the range from 1 to result
            random_number = random.randint(1, int(result))

            # Subtracting random_number from the current value
            current_value -= random_number

            # Adding the transaction to the list
            transactions.append(random_number)

            # Decrementing the divisor
            divisor -= 1

        # Checking if divisor reached 2
        if divisor == 2:
            # Calculating the remaining value and split it into two parts
            remaining_value = target_number - sum(transactions)
            first_part = remaining_value // 4
            second_part = remaining_value - first_part

            # Add the two parts to the list
            transactions.extend([first_part, second_part])
        else:
            # If divisor is 1, just adding the remaining value to the list
            transactions.append(current_value)

    except Exception as e:
        print(f"Error occurred: {e}")

    return transactions

def create_transactions_one_day(date, transaction_lists):
    random_transactions = []  # List to store transactions with random times

    try:
        for amount in transaction_lists:
            # Generate a random time within the day
            random_hour = random.randint(0, 23)
            random_minute = random.randint(0, 59)
            random_second = random.randint(0, 59)
            random_time = datetime.datetime.combine(date, datetime.time(random_hour, random_minute, random_second))
            random_transactions.append((random_time, amount))
    except Exception as e:
        print(f"Error occurred: {e}")

    # Create pandas DataFrame from the list of tuples
    df = pd.DataFrame(random_transactions, columns=['Timestamp', 'Amount'])

    return df
