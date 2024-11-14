import os
import pandas as pd
import sqlite3

# Step 1: Define input URLs for datasets
input_urls = [
    'https://data.cdc.gov/api/views/9dzk-mvmi/rows.csv?accessType=DOWNLOAD',
    'https://data.cdc.gov/api/views/bxq8-mugm/rows.csv?accessType=DOWNLOAD'
]

# Step 2: Directory setup for storing processed data
output_dir = './data'
os.makedirs(output_dir, exist_ok=True)
database_path = os.path.join(output_dir, 'Diabetes_data.sqlite')

def load_datasets(urls):
    """
    Load datasets from given URLs into a list of DataFrames.
    """
    dataframes = []
    for url in urls:
        df = pd.read_csv(url)
        dataframes.append(df)
    return dataframes

def preprocess_data(dataframes):
    """
    Preprocess each DataFrame: select specific columns, drop missing values, and convert types.
    """
    processed_data = []
    columns_to_keep = ['Year', 'Month', 'Diabetes Mellitus']
    
    for df in dataframes:
        df = df[columns_to_keep].dropna()
        df['Year'] = df['Year'].astype(int)
        df['Month'] = df['Month'].astype(int)
        processed_data.append(df)
    
    return pd.concat(processed_data, ignore_index=True)

def save_to_sqlite(data, database_path):
    """
    Save the processed DataFrame to an SQLite database.
    """
    with sqlite3.connect(database_path) as conn:
        data.to_sql('diabetes_data', conn, if_exists='replace', index=False)
    print(f"Data saved to SQLite database at {database_path}")

def main():
    # Load datasets
    dataframes = load_datasets(input_urls)
    
    # Preprocess data
    combined_data = preprocess_data(dataframes)
    
    # Save to SQLite
    save_to_sqlite(combined_data, database_path)

if __name__ == "__main__":
    main()
