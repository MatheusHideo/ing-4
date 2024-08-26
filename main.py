from extractCSV import extract_bancos, extract_empregados, extract_reclamacoes
from transformLoad import load, transform_bancos, transform_empregados, transform_reclamacoes, merge_files, transforming_load_silver
from config import mysql_db


def main():
    try:
        print('Configuring Database')
        # db.configure()
        mysql_db.configure()

        print('Extracting bancos files')
        extract_bancos.execute()

        print('Extracting  empregados files')
        extract_empregados.execute()

        print('Extracting  reclamacoes files')
        extract_reclamacoes.execute()

        print('Transforming bancos')
        transform_bancos.execute()

        print('Transforming empregados')
        transform_empregados.execute()

        print('Transforming reclamacoes')
        transform_reclamacoes.execute()
        
        print("loading Silver")
        transforming_load_silver.execute()

        print('Making the old trusted')
        merge_files.execute()

    except Exception as err:
        print(err)
        raise err


main()
