"""Configuration File"""

data_path = r'./data'

raw_empregados_path = rf'{data_path}/raw/Empregados'

raw_bancos_path = rf'{data_path}/raw/Bancos'

raw_reclamacoes_path = rf'{data_path}/raw/Reclamacoes'

bronze_path = rf'{data_path}/bronze'

silver_path = rf'{data_path}/silver'

gold_path = rf'{data_path}/gold'

sqlite_path = 'database'

mysql = {
    'host': 'localhost',
    'port': '3306',
    'user': 'root',
    'password': 'root',
    'database': 'eEDB011'
}
postgres = {
    'host': 'localhost',
    'port': '3306',
    'user': 'root',
    'password': 'root',
    'database': 'eedb011'
}