import psycopg2
import json
from decimal import Decimal

def decimal_converter(obj):
    if isinstance(obj, Decimal):
        return float(obj)  # Convertendo Decimal para float
    raise TypeError

# Função para conectar ao banco de dados e extrair os dados da tabela
def fetch_table_data(host, database, user, password, table_name):
    # Conexão com o banco de dados
    connection = psycopg2.connect(host=host, database=database, user=user, password=password)
    cursor = connection.cursor()

    # Consulta SQL para obter os dados da tabela
    query = f"SELECT * FROM {table_name};"
    cursor.execute(query)

    # Descrição das colunas
    columns = [desc[0] for desc in cursor.description]

    # Extração dos dados da tabela como dicionários
    table_data = []
    for row in cursor.fetchall():
        row_data = {}
        for i, value in enumerate(row):
            if isinstance(value, Decimal):
                value = float(value)  # Convertendo Decimal para float
            row_data[columns[i]] = value
        table_data.append(row_data)

    # Fechamento do cursor e da conexão
    cursor.close()
    connection.close()

    return table_data

# Configurações de conexão com o banco de dados
host = 'silly.db.elephantsql.com'  # Substitua 'your_host' pelo host do seu banco de dados
database = 'lzqcvgyy'  # Substitua 'your_database' pelo nome do seu banco de dados
user = 'lzqcvgyy'  # Substitua 'your_username' pelo seu nome de usuário do banco de dados
password = 'CdqL6VVFbrmaKawLkVV6g9GSdWpmkIpU'  # Substitua 'your_password' pela sua senha do banco de dados
# table_name = 'classroom'  # Substitua 'your_table' pelo nome da tabela que você quer exportar

tabelas = [
    "advisor",
    "classroom",
    "course",
    "department",
    "instructor",
    "prereq",
    "section",
    "student",
    "takes",
    "teaches",
    "time_slot"
]
for table_name in tabelas:
    # Obtendo os dados da tabela
    table_data = fetch_table_data(host, database, user, password, table_name)

    # Salvando os dados em um arquivo JSON
    json_file = table_name + '.json'
    with open(json_file, 'w') as file:
        json.dump(table_data, file, default=decimal_converter, indent=4)  # Usando a função de conversão para Decimal

    print(f"Os dados da tabela '{table_name}' foram salvos no arquivo '{json_file}'.")