import os
import pandas as pd
import pymysql
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load environment variables from .env
load_dotenv()

# Retrieve credentials
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
database = os.getenv("DB_NAME")

# Create SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

directory = "pizza_proj/"
file_dict = {}

for file in os.listdir(directory):
    if file.endswith(".csv"):
        file_dict[file[:file.index('.')]] = directory + file

# Loop through dictionary
for name, path in file_dict.items():
    try:
        # Read CSV into DataFrame
        df = pd.read_csv(path)

        # Insert into MySQL table
        df.to_sql(name, con=engine, if_exists="replace", index=False)

        print(f"Successfully imported {path} into {name}")
    except Exception as e:
        print(f"Error importing {path}: {e}")
