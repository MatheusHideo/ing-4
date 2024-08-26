import json
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from config.define import silver_path, bronze_path

# Função para corrigir o JSON
def fix_json(input_file, output_file):
    with open(input_file, 'r', encoding='cp1252') as file:
        lines = file.readlines()

    with open(output_file, 'w', encoding='cp1252') as outfile:
        for i, line in enumerate(lines):
            line = line.strip()
            if line.endswith("}") and i < len(lines) - 2:
                outfile.write(line + ",\n")
            else:
                outfile.write(line + "\n")
        
    print(f"JSON corrigido salvo em: {output_file}")

# Função para carregar e processar o JSON e salvar como Parquet
def process_json_to_parquet(json_path):
    # Carregar os dados JSON
    with open(json_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    # Converter o JSON em um DataFrame
    df = pd.DataFrame(data)

    # Substituir 'null' por None para tratamento correto de valores nulos
    df.replace('null', None, inplace=True)

    # Validação e Conversão de Tipos
    try:
        df['employer-founded'] = pd.to_numeric(df['employer-founded'], errors='coerce')
    except Exception as e:
        print(f"Erro ao converter employer-founded: {e}")

    # Filtrar registros válidos (se necessário)
    valid_df = df

    # Converter o DataFrame em uma tabela PyArrow
    table = pa.Table.from_pandas(valid_df)

    # # Gerar um nome de arquivo único usando o timestamp atual
    # timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    # parquet_file_path = f'data/silver/empregados_{timestamp}.parquet'

    parquet_file_path = os.path.join(f'{silver_path}/empregados.parquet')
    # Escrever a tabela em um arquivo Parquet/TXT
    pq.write_table(table, parquet_file_path)

    # print(f"Arquivo Parquet salvo em: {parquet_file_path}")
    
    # Ler o arquivo Parquet/TXT em um DataFrame do Pandas
    df = pq.read_table(parquet_file_path).to_pandas()

    # Salvar em TXT (formato personalizado, exemplo separado por tabulações)
    valid_df.replace('null', pd.NA).replace('', pd.NA).replace(' ', pd.NA)
    valid_df.fillna(pd.NA)
    valid_df.to_parquet(parquet_file_path)
    print(f"Arquivo Parquet salvo em: {parquet_file_path}")
    
    # Visualizar as primeiras linhas do DataFrame
    print(df.head())

def execute():
    # Caminho para o arquivo JSON
    json_file_path = f'{bronze_path}/empregados.json'
    fixed_json_file_path = f'{bronze_path}/empregados_corrected.json'
    
    # Corrigir o JSON e processar
    fix_json(json_file_path, fixed_json_file_path)
    process_json_to_parquet(fixed_json_file_path)
