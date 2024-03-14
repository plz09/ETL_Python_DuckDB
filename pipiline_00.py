import os
import pandas as pd
import gdown
import duckdb
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

from datetime import datetime

load_dotenv()

def conn_database():
    """Connect to DuckDB databaase; Create if not exist."""
    return duckdb.connect(database='duckdb.db', read_only=False)

def init_table(con):
    """Create table if not exists."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS files_history (
            file_name VARCHAR,
            process_time TIMESTAMP
        )
    """)


def register_file(con, file_name):
    """Register a new file in the database with datetime now()."""
    con.execute("""
        INSERT INTO files_history (file_name, process_time)
        VALUES (?, ?)
    """, (file_name, datetime.now()))


def processed_files(con):
    """Returns a set with the names of all files already processed."""
    return set(row[0] for row in con.execute("SELECT file_name FROM files_history").fetchall())



def download_files_gdown(url_folder, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    gdown.download_folder(url_folder, output=local_dir, quiet=False, use_cookies=False)


def list_csv_files(directory):
    csv_files = []
    all_files = os.listdir(directory)
    for file in all_files:
        if file.endswith(".csv"):
            complete_path = os.path.join(directory, file)
            csv_files.append(complete_path)
    return csv_files


def read_csv(file_path):
    dataframe_duckdb = duckdb.read_csv(file_path)
    return dataframe_duckdb


def transform(df: DuckDBPyRelation) -> DataFrame:
    df_transformed = duckdb.sql("SELECT *, quantidade * valor AS total_vendas FROM df").df()
    return df_transformed


# Function to convert the Duckdb in Pandas and save the DataFrame on PostgreSQL
def save_to_postgres(df_duckdb, param_table):
    DATABASE_URL = os.getenv("DATABASE_URL")  
    engine = create_engine(DATABASE_URL)
    # Save the DataFrame on PostgreSQL
    df_duckdb.to_sql(param_table, con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    url_folder = 'https://drive.google.com/drive/folders/1maqV7E3NRlHp12CsI4dvrCFYwYi7BAAf'
    local_dir = './folder_gdown'
    #download_files_gdown(url_folder, local_dir)
    files_list = list_csv_files(local_dir)
    con = conn_database()
    init_table(con)
    processed = processed_files(con)

    for file_path in files_list:
        file_name = os.path.basename(file_path)
        if file_name not in processed:

            dataframe_duckdb = read_csv(file_path)
            pandas_df_transformed =  transform(dataframe_duckdb)
            register_file(con, file_name)
            print(f"Arquivo {file_name} processado e salvo.")
            save_to_postgres(pandas_df_transformed, 'vendas_calculado')
        else:
            print(f"Arquivo {file_name} já foi processado anteriormente.")