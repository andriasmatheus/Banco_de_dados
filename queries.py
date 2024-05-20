from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import psycopg2
import json
from decimal import Decimal


host = 'motty.db.elephantsql.com'  
database = 'gkfzpfqp'  
user = 'gkfzpfqp'  
password = 'LwcjHznbhkgs5SSRTMPZu56Ug3z7yerw'  

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

# Credenciais do banco de dados MongoDB
USERNAME_MONGO = 'andriasmatheus'
PASSWORD_MONGO = 'senha'
HOST_MONGO = 'banconovo.poi7cyk.mongodb.net'
PORT_MONGO = 27017  # Porta padrão do MongoDB

# URI de conexão ao banco de dados
uri = f"mongodb+srv://{USERNAME_MONGO}:{PASSWORD_MONGO}@{HOST_MONGO}"

# Set the Stable API version when creating a new client
client = MongoClient(uri, server_api=ServerApi('1'))
schema = client["BancoNovo"]

def drop_all_tables():
    try:
        # Listar todas as coleções no banco de dados
        collections = schema.list_collection_names()

        # Remover todas as coleções
        for collection_name in collections:
            schema.drop_collection(collection_name)
            print(f"A coleção '{collection_name}' foi removida com sucesso.")

        print(f"Todas as coleções foram removidas do banco de dados.")

    except Exception as e:
        print(f"Ocorreu um erro ao tentar remover as coleções do banco de dados: {e}")

    finally:
        # Fechar a conexão
        if client:
            client.close()

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


def envia_dados_do_post_para_o_mongo():
    for table_name in tabelas:
        # Obtendo os dados da tabela
        table_data = fetch_table_data(host, database, user, password, table_name)

        colecao = schema[table_name]

        #Inserir os dados na coleção
        colecao.insert_many(table_data)

def query1():
    # 1. Listar todos os cursos oferecidos por um determinado departamento
    DEPARTAMENT = "Mech. Eng."
    collection = schema["course"]

    results = collection.find({ "dept_name": DEPARTAMENT})
    todos_os_cursos = []

    for t in results:
        todos_os_cursos.append(t['title'])
    
    if(len(todos_os_cursos) > 0):
        print(f"Os cursos oferecidos pelo departamento de {DEPARTAMENT} são:")
        for curso in todos_os_cursos:
            print(curso)
    else:
        print(f"Não há nenhum resultado de curso oferecido pelo departamento {DEPARTAMENT}.")

def query2(semester = "Fall"):
    # 2. Recuperar todas as disciplinas de um curso específico em um determinado semestre

    SEMESTER = semester
    collection = schema['section']
    sections = collection.find({"semester": SEMESTER})

    courses_id = []
    for section in sections:
        courses_id.append(section['course_id'])

    collection2 = schema['course']
    courses = collection2.find()

    courses_title = []
    for course in courses:
        for course_id in courses_id:
            if course['course_id'] == course_id:
                courses_title.append(course['title'])
            
    if(len(courses_title) > 0):
        print(f"O(s) curso(s) do semestre {SEMESTER} é(são):")
        for title in courses_title:
            print(title)
    else:
        print(f"Não possui nenhum registro de curso no semestre {SEMESTER}")

def find_all_students_who_are_enrolled_in_a_specific_course(course_id = "802"):
    # 3. Encontrar todos os estudantes que estão matriculados em um curso específico

    COURSE_ID = course_id
    collection3 = schema["takes"]
    takes = collection3.find({ "course_id": COURSE_ID})

    collection4 = schema["student"]
    students = collection4.find()

    # Criar uma lista para armazenar os nomes dos alunos matriculados no curso específico
    students_names = []

    # Recuperar os IDs dos alunos matriculados no curso específico
    takes_ids = [take['id'] for take in takes]

    # Iterar sobre os alunos e verificar se o ID está na lista de IDs dos alunos matriculados no curso específico
    for student in students:
        if student['id'] in takes_ids:
            students_names.append(student['name'])

    courseCollection = schema["course"]
    course = courseCollection.find({"course_id": COURSE_ID})
    for k in course:
        nome_curso = k['title']
    print(f"Os alunos matriculados no curso {nome_curso}, são:")
    # Imprimir os nomes dos alunos matriculados no curso específico
    for name in students_names:
        print(name)

def average_salaries_of_teachers_in_a_given_department(dept_name = "Cybernetics"):
    # 4. Listar a média de salários de todos os professores em um determinado departamento
    DEPARTAMENT_NAME = dept_name
    collection = schema["instructor"]
    instrutores = collection.find({"dept_name": DEPARTAMENT_NAME})
    salarios = []
    for t in instrutores:
        salarios.append(t['salary'])

    total = 0
    media = 0
    for salario in salarios:
        total += float(salario)
    media = total/len(salarios)

    print("A média dos salários dos professores de Cybernetics é: {:.2f}".format(media))

def total_number_of_credits_earned_by_a_specific_student(student_id = "84702"):
    # 5. Recuperar o número total de créditos obtidos por um estudante específico

    STUDENT_ID = student_id
    collection = schema['student']
    alunos = collection.find({"id": STUDENT_ID})

    total_credito = 0
    for aluno in alunos:
        total_credito += float(aluno['tot_cred'])
        aluno_nome = aluno['name']

    print(f"Total de crédito obtido do aluno {aluno_nome}:", total_credito)

def find_all_courses_taught_by_a_professor_in_a_specific_semester(semester = "Fall", teacher_id = "28097"):
    # 6. Encontrar todas as disciplinas ministradas por um professor em um semestre específico

    SEMESTER = semester
    TEACHER_ID = teacher_id
    teaches_collection = schema['teaches']
    teaches_de_um_professor = teaches_collection.find({"id": TEACHER_ID}) # Todos as disciplinas ministradas por um professor específico

    info = []
    for t in teaches_de_um_professor:
        info.append({'course_id': t['course_id'], 'semester': t['semester']}) # Armazena curso e semestre ministrado por um professor

    course_collection = schema['course']
    courses = course_collection.find()

    resultado_query = []
    for course in courses:
        for course_info in info:
            if course['course_id'] == course_info['course_id'] and SEMESTER in course_info['semester']:
                resultado_query.append(course['title'])

    if(len(resultado_query) > 0):
        print(f"Curso ministrado pelo professor de id {TEACHER_ID} no semestre {SEMESTER}:")
        for resultado in resultado_query:
            print(resultado)
    else:
        print(f"Não foi encontrado nenhum resultado para o professor de id {TEACHER_ID} e semestre {SEMESTER}")

def all_students_who_have_a_specific_teacher_as_their_advisor(teacher_id = "19368"):
    # 7. Listar todos os estudantes que têm um determinado professor como orientador

    TEACHER_ID = teacher_id
    collection = schema['advisor']
    advisors = collection.find({"i_id": TEACHER_ID})

    students = []
    for t in advisors:
        students.append(t['s_id'])

    if(len(students) > 0):
        for student in students:
            print("Aluno:", student)
    else:
        print(f"Não foi encontrado nenhum resultado de busca de aluno para o professor de id {TEACHER_ID}")


def find_all_prerequisites_for_a_specific_course(course_id = "376"):
    # 9. Encontrar todos os pré-requisitos de um curso específico

    COURSE_ID = course_id

    collection = schema['prereq']
    prereqs = collection.find({"course_id": course_id})

    prereqs_list = []
    for t in prereqs:
        prereqs_list.append(t['prereq_id'])

    if(len(prereqs_list) > 0):
        print(f"O(s) pré-requisito(s) para o curso de id {COURSE_ID}, é(são):")
        for prereq in prereqs_list:
            print(prereq)
    else:
        print("Nenhum pré-requisito encontrado para o curso selecionado")


def recover_the_number_of_students_guided_by_each_teacher():
    # 10. Recuperar a quantidade de alunos orientados por cada professor

    collection = schema['advisor']
    advisors = collection.find()

    lista_professores = {}
    for advisor in advisors:
        if advisor['i_id'] not in lista_professores:
            lista_professores[advisor['i_id']] = 0

    lista_resultado = {}
    for professor in lista_professores:
        lista_resultado[professor] = 0; # Inicializa todo o dicionário

    advisors = collection.find()
    for k in advisors:
        if 'i_id' in advisor:
            lista_resultado[k['i_id']] += 1 

    for professor in lista_professores:
        qtd = lista_resultado[professor]
        print(f"O professor de id {professor} orienta {qtd} aluno")


try:
    # drop_all_tables()

    ### ---------------------------------------------------------------------------------------------------------------- ###
    ###                        Criação das tabelas e inserção dos dados no cassandra:                                    ###
    ### ---------------------------------------------------------------------------------------------------------------- ###
    # envia_dados_do_post_para_o_mongo() # Essa função deve ser executada apenas uma vez após executar a drop_all_tables()

    ### ---------------------------------------------------------------------------------------------------------------- ###
    ###                                     Descomente a query a ser usada:                                              ###
    ### ---------------------------------------------------------------------------------------------------------------- ###
    # 1. Listar todos os cursos oferecidos por um determinado departamento
    # query1()

    # 2. Recuperar todas as disciplinas de um curso específico em um determinado semestre
    # query2()

    # 3. Encontrar todos os estudantes que estão matriculados em um curso específico
    # find_all_students_who_are_enrolled_in_a_specific_course()

    # 4. Listar a média de salários de todos os professores em um determinado departamento
    # average_salaries_of_teachers_in_a_given_department()

    # 5. Recuperar o número total de créditos obtidos por um estudante específico
    # total_number_of_credits_earned_by_a_specific_student()

    # 6. Encontrar todas as disciplinas ministradas por um professor em um semestre específico
    # find_all_courses_taught_by_a_professor_in_a_specific_semester()

    # 7. Listar todos os estudantes que têm um determinado professor como orientador
    # all_students_who_have_a_specific_teacher_as_their_advisor()

    # 8. Recuperar todas as salas de aula sem um curso associado

    # 9. Encontrar todos os pré-requisitos de um curso específico
    # find_all_prerequisites_for_a_specific_course()

    # 10. Recuperar a quantidade de alunos orientados por cada professor
    # recover_the_number_of_students_guided_by_each_teacher()

    client.close()

except Exception as e:
    print(e)