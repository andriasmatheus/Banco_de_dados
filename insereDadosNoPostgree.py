import psycopg2

host = 'motty.db.elephantsql.com'  
database = 'gkfzpfqp'  
user = 'gkfzpfqp'  
password = 'LwcjHznbhkgs5SSRTMPZu56Ug3z7yerw'  

connectionSQL = psycopg2.connect(host=host, database=database, user=user, password=password)

arquivo_insere_dados_na_tabela = "smallRelationsInsertFile.sql"

def insereNoPostgree():
    try:
        queries = []
        file = open(arquivo_insere_dados_na_tabela, "r")
        sqlFile = file.read()
        file.close()
        sqlCommand = sqlFile.split(";")

        for query in sqlCommand:
            cursorSQL = connectionSQL.cursor()
            try:
                cursorSQL.execute(query)
            except:
                pass
            connectionSQL.commit()
            cursorSQL.close()

        connectionSQL.close()

    except FileNotFoundError:
        # Se o arquivo não for encontrado, imprima uma mensagem de erro
        print("O arquivo '{}' não foi encontrado.".format(arquivo_insere_dados_na_tabela))

    except Exception as e:
        # Se ocorrer qualquer outro erro, imprima uma mensagem de erro geral
        print("Ocorreu um erro:", e)
