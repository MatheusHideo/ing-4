
# Exercício 4 - Ingestão de Dados

Este repositório contém o código e a documentação para o **Exercício 4** da disciplina de Ingestão de Dados do curso de Engenharia de Dados.

## Objetivo

O objetivo deste exercício é desenvolver uma pipeline de ingestão de dados utilizando linguagem de programação Python para ingestão e tratamento de dados e para processo de transformação utilizar a ferramenta DBT.

## Estrutura do Repositório

- **`analyse/`**: Contém Query de consultas.
- **`data/`**: Contém os arquivos de dados utilizados para o exercício (bronze/, silver/, gold/, raw/).
- **`extractCSV/`**: Contém scripts Python para automatização das tarefas de ETL - extração.
- **`TransformLoad/`**: Contém scripts de teste para validação das transformações e cargas de dados - transformação e load no SQLite, MySQL e PostgresSQL.
- **`config`**: Contém script de criação do db, insert no banco a partir do file na camada gold.
- **`transformLoadDBT`**: Contém a estrutura para a transformação dos dados e carga no banco de dados MySQL
- **`transformLoadPostgresDBT`**: Contém a estrutura para a transformação dos dados e carga no banco de dados PostgresSQL

## Tecnologias Utilizadas

- **Python**: Linguagem de programação utilizada para desenvolvimento dos scripts.
- **`MySQl, PostgresSQL`**: Utilizado criação do banco de dados e análise de dados estruturados.
- **DBT**: Utilizado para transformação e carga dos dados gerados pelo Python 

## Como Executar

1. Clone este repositório para sua máquina local:

   ```bash
   git clone https://github.com/MatheusHideo/ing-4.git
   cd ing-4
   ```
2. Execute o comando abaixo para instalar os requirimentos:

   ```bash
      pip install -r ./requirements.txt
   ```

3. Copie o profile.xml para sua pasta local do DBT tome cuidado isso irá sobreescrever suas configurações:
   ```bash
      cp ~/.dbt/profiles.xml ~/.dbt/profile.old.yml 
      cp profiles.yml ~/.dbt/
   ```

4. Configure seus caminhos e conexões no arquivo config/define


5. Extraia os dados a camada Silver e a carrege no banco:
   ```bash
      python main.py
   ```

6. Execute o DBT do postgres:
   ```bash
      cd transformLoadPostgresDBT
      dbt run
   ```

7. Execute o DBT do mysql:
   ```bash
      cd ../transformLoadDBT
      dbt run
   ```

## Contribuições

Sinta-se à vontade para abrir issues ou enviar pull requests para contribuir com melhorias ou correções.

