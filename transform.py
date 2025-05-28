import pandas as pd
import os

def clean_data():
    df = pd.read_csv("data/ecommerce_raw.csv", nrows=100000)  # sample for speed
    df['event_time'] = pd.to_datetime(df['event_time'])

    # Optional: Filter purchases only
    df = df[df['event_type'] == 'purchase']

    # Add revenue simulation
    df['price'] = 100 + (df['product_id'] % 50) * 5
    df['revenue'] = df['price']

    # Save cleaned file
    df.to_csv("output/cleaned_data.csv", index=False)
    print("✅ Data cleaned and saved.")
