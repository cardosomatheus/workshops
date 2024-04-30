import os
import duckdb
import gdown
import glob
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from duckdb import DuckDBPyRelation
from pandas import DataFrame


load_dotenv()
def baixar_os_arquivos_do_google_driver(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False)
    

def listar_arquivos_csv(url_pasta):
    url_pasta = url_pasta +'/*.csv'
    all_paths = glob.glob(url_pasta)
    return all_paths
  
def tranfromar_em_dataframe_pandas(df: DuckDBPyRelation) -> DataFrame: 
    df_transformador = duckdb.sql("""SELECT * FROM df""").df()
    return df_transformador 

    
def ler_csv(arquivo):
    return duckdb.read_csv(arquivo)


def salvar_no_postgres(df_duckdb, tabela):

    DATABASE_URL = os.getenv('DATABASE_URL')
    engine = create_engine(DATABASE_URL)
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)
    
    
if __name__ == "__main__":

    url_pasta = 'https://drive.google.com/drive/folders/1kR73CmaVR45Wh7ZCig5Y4wqKDQKbOh1s'
    diretorio_local = './pasta_gdwon'
    #baixar_os_arquivos_do_google_driver(url_pasta=url_pasta, diretorio_local=diretorio_local)
    for arquivo in listar_arquivos_csv(diretorio_local):
        dataframe_duckdb = ler_csv(arquivo)
        dataframe_pandas = tranfromar_em_dataframe_pandas(dataframe_duckdb)
        salvar_no_postgres(dataframe_pandas, 'vendas')

