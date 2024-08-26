import json
import pandas as pd
import os
from datetime import datetime
from config.define import bronze_path, silver_path

def fix_json(input_file, output_file):
    with open(input_file, 'r', encoding='cp1252') as file:
        lines = file.readlines()

    with open(output_file, 'w', encoding='cp1252') as outfile:
        outfile.write("[\n")
        for i, line in enumerate(lines):
            line = line.strip()
            if line.endswith("}") and i < len(lines) - 1:
                outfile.write(line + ",\n")
            else:
                outfile.write(line + "\n")
        outfile.write("]\n")
        
    print(f"JSON corrigido salvo em: {output_file}")

def process_json_to_csv_parquet_and_txt(json_path, output_dir):
    with open(json_path, 'r', encoding='cp1252') as file:
        data = json.load(file)

    if not data:
        print("JSON limpo está vazio.")
        return

    df = pd.DataFrame(data)
    df.replace('null', None, inplace=True)

    if 'Nome' in df.columns:
        df['Nome'] = df['Nome'].str.replace('- PRUDENCIAL', '', regex=False)
    else:
        print("Aviso: Coluna 'Nome' não encontrada no DataFrame.")

    if 'CNPJ' in df.columns:
        df['CNPJ'] = df['CNPJ'].astype(str)
    else:
        print("Aviso: Coluna 'CNPJ' não encontrada no DataFrame.")

    valid_df = df

    # # Gerar um nome de arquivo único usando o timestamp atual
    # timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    # Salvar em TXT (formato personalizado, exemplo separado por tabulações)
    parquet_file_path = os.path.join(output_dir, f'bancos.parquet')
    
    valid_df.replace('null', pd.NA).replace('', pd.NA).replace(' ', pd.NA)
    valid_df.fillna(pd.NA)
    
    valid_df.to_parquet(parquet_file_path)
    print(f"Parquett salvo em: {parquet_file_path}")

    # Visualizar as primeiras linhas do DataFrame
    print(valid_df.head())


def execute():
    input_file = f'{bronze_path}/bancos.json'
    output_file = f'{bronze_path}/bancos_corrected.json'
    fix_json(input_file, output_file)
    json_file_path = f'{bronze_path}/bancos_corrected.json'
    process_json_to_csv_parquet_and_txt(json_file_path, silver_path)
