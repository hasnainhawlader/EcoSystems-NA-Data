import os
import pandas as pd
import sqlite3

# Step 1: File paths for input datasets
input_files = [
    'https://data.cdc.gov/api/views/9dzk-mvmi/rows.csv?accessType=DOWNLOAD',
    'https://data.cdc.gov/api/views/bxq8-mugm/rows.csv?accessType=DOWNLOAD'
]

# Step 2: Directory to store the processed data
output_dir = './data'
os.makedirs(output_dir, exist_ok=True)  # Create directory if it doesn't exist
output_file = os.path.join(output_dir, 'Diabetes_data.sqlite')

# Step 3: Read and combine datasets
def load_and_clean_data(files):
    dataframes = []
    for file in files:
        df = pd.read_csv(file)
        # Keep relevant columns
        columns_to_keep = ['Year', 'Month', 'Diabetes Mellitus']
        df = df[columns_to_keep]
        df = df.dropna()  # Drop rows with missing values
        df['Year'] = df['Year'].astype(int)  # Ensure Year is integer
        df['Month'] = df['Month'].astype(int)  # Ensure Month is integer
        dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)

# Step 4: Load data
combined_data = load_and_clean_data(input_files)

# Step 5: Save data to SQLite
def save_to_sqlite(data, database_path):
    with sqlite3.connect(database_path) as conn:
        data.to_sql('suicide_data', conn, if_exists='replace', index=False)
    print(f"Data saved to SQLite database at {database_path}")

save_to_sqlite(combined_data, output_file)