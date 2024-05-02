import os
import duckdb
import gdown
import glob
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from duckdb import DuckDBPyRelation
from pandas import DataFrame
from datetime import datetime

load_dotenv()
def conectar_banco():
    """Conecta ao banco de dados DuckDB; cria o banco se não existir."""
    return duckdb.connect(database='duckdb.db', read_only=False)

def inicializar_tabela(con):
    """Cria a tabela se ela não existir."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS historico_arquivos (
            nome_arquivo VARCHAR,
            horario_processamento TIMESTAMP
        )
    """)

def registrar_arquivo(con, nome_arquivo):
    """Registra um novo arquivo no banco de dados com o horário atual."""
    con.execute("""
        INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
        VALUES (?, ?)
    """, (nome_arquivo, datetime.now()))

def arquivos_processados(con):
    """Retorna um set com os nomes de todos os arquivos já processados."""
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall())

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
    aquicos_csv_listados = listar_arquivos_csv(diretorio_local)
    conexao_banco = conectar_banco()
    nome_tabela = 'vendas'
    inicializar_tabela(con=conexao_banco)
    arquivos_processados_listados = arquivos_processados(con=conexao_banco)
    
    #baixar_os_arquivos_do_google_driver(url_pasta=url_pasta, diretorio_local=diretorio_local)
    
    for arquivo in aquicos_csv_listados:
        if arquivo not in arquivos_processados_listados:
            dataframe_duckdb = ler_csv(arquivo)
            dataframe_pandas = tranfromar_em_dataframe_pandas(dataframe_duckdb)
            salvar_no_postgres(dataframe_pandas, nome_tabela)
            print('O arquivo', arquivo, ' processado com sucesso')
            registrar_arquivo(con=conexao_banco, nome_arquivo=arquivo)
        else:
            print('O arquivo', arquivo, ' Já havia sido processado anteriormente.')
