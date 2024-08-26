from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from config.define import mysql, silver_path, postgres


def load_mysql(df, table: str ,mysql_info = mysql):
    conn =  create_engine(f'mysql+mysqlconnector://{mysql_info['user']}:{mysql_info['password']}@{mysql_info['host']}/{mysql_info['database']}')
    df.to_sql(table, conn, if_exists='replace', index=False)

def load_postgres(df, table: str ,mysql_info = mysql):
    print(f'postgresql+psycopg2://{mysql_info['user']}:{mysql_info['password']}@{mysql_info['host']}/{mysql_info['database']}')
    conn =  create_engine(f'postgresql+psycopg2://{mysql_info['user']}:{mysql_info['password']}@{mysql_info['host']}/{mysql_info['database']}')
    try: 
        df.to_sql(table, conn, if_exists='replace', index=False, schema="eedb011")
    except Exception as e:
        print(e)


def read_parquet(path, dtypes):
    untyped_df = pd.read_parquet(f"{silver_path}/{path}.parquet")
    untyped_df = untyped_df.replace('null', pd.NA).replace('', pd.NA).replace(' ', pd.NA)
    untyped_df = untyped_df.fillna(pd.NA)
     
    return pd.DataFrame(untyped_df, columns=dtypes)

def execute():
    bancos_dtypes = {
        "Segmento": str,
        "CNPJ": str,
        "Nome": str
    }

    empregados_dtypes =  {
        "employer_name": str,
        "reviews_count": int,
        "culture_count": int,
        "salaries_count": int,
        "benefits_count": int,
        "employer-website": str,
        "employer-headquarters": str,
        "employer-founded": int,
        "employer-industry": str,
        "employer-revenue": str,
        "url": str,
        "Geral": float,
        "Cultura e valores": float,
        "Diversidade e inclusão": float,
        "Qualidade de vida": float,
        "Alta liderança": float,
        "Remuneração e benefícios": float,
        "Oportunidades de carreira": float,
        "Recomendam para outras pessoas(%)": float,
        "Perspectiva positiva da empresa(%)": float,
        "Segmento": str,
        "Nome": str,
        "match_percent": int,
        "CNPJ": str
    }

    reclamacoes_dtypes = {
        "Ano": int,
        "Trimestre": str,
        "Categoria": str,
        "Tipo": str,
        "CNPJ": str,
        "Instituição financeira": str,
        "Índice": str,
        "Quantidade de reclamações reguladas procedentes": int,
        "Quantidade de reclamações reguladas - outras": int,
        "Quantidade de reclamações não reguladas": int,
        "Quantidade total de reclamações": int,
        "Quantidade total de clientes – CCS e SCR": int,
        "Quantidade de clientes – CCS": int,
        "Quantidade de clientes – SCR": int
    }

    df_to_load = [{"table": 'bancos', "data": read_parquet('bancos', bancos_dtypes)},
                 {"table": "empregados", "data":  read_parquet('empregados', empregados_dtypes)},
                  {"table": "reclamacoes", "data": read_parquet('reclamacoes', reclamacoes_dtypes)}]

    for df in df_to_load:
        load_mysql(df['data'], df['table'])
        load_postgres(df['data'], df['table'], postgres)
