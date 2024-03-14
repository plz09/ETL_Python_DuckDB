import os
import pandas as pd
import gdown
import duckdb
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

load_dotenv()


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


# Função para converter o Duckdb em Pandas e salvar o DataFrame no PostgreSQL
def save_to_postgres(df_duckdb, param_table):
    DATABASE_URL = os.getenv("DATABASE_URL")  
    engine = create_engine(DATABASE_URL)
    # Salvar o DataFrame no PostgreSQL
    df_duckdb.to_sql(param_table, con=engine, if_exists='append', index=False)


if __name__ == '__main__':
    url_folder = 'https://drive.google.com/drive/folders/1maqV7E3NRlHp12CsI4dvrCFYwYi7BAAf'
    local_dir = './folder_gdown'
    #download_files_gdown(url_folder, local_dir)
    files_list = list_csv_files(local_dir)

    for file_path in files_list:
        dataframe_duckdb = read_csv(file_path)
        pandas_df_transformed =  transform(dataframe_duckdb)
        save_to_postgres(pandas_df_transformed, 'vendas_calculado')