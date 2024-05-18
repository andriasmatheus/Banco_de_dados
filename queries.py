from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Credenciais do banco de dados
USERNAME = 'andriasmatheus'
PASSWORD = 'senha'
HOST = 'banconovo.poi7cyk.mongodb.net'
PORT = 27017  # Porta padrão do MongoDB

# URI de conexão ao banco de dados
uri = f"mongodb+srv://{USERNAME}:{PASSWORD}@{HOST}"

# Set the Stable API version when creating a new client
client = MongoClient(uri, server_api=ServerApi('1'))

def query1():
    # 1. Listar todos os cursos oferecidos por um determinado departamento
    DEPARTAMENT = "Mech. Eng."
    collection = database["course"]

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

def query3():
    # 3. Encontrar todos os estudantes que estão matriculados em um curso específico
    collection3 = database["takes"]
    takes = collection3.find({ "course_id": "802"})

    collection4 = database["student"]
    students = collection4.find()

    # Criar uma lista para armazenar os nomes dos alunos matriculados no curso específico
    students_names = []

    # Recuperar os IDs dos alunos matriculados no curso específico
    takes_ids = [take['id'] for take in takes]

    # Iterar sobre os alunos e verificar se o ID está na lista de IDs dos alunos matriculados no curso específico
    for student in students:
        if student['id'] in takes_ids:
            students_names.append(student['name'])

    courseCollection = database["course"]
    course = courseCollection.find({"course_id": "802"})
    for k in course:
        nome_curso = k['title']
    print(f"Os alunos matriculados no curso {nome_curso}, são:")
    # Imprimir os nomes dos alunos matriculados no curso específico
    for name in students_names:
        print(name)

def query4():
    # 4. Listar a média de salários de todos os professores em um determinado departamento
    collection = database["instructor"]
    instrutores = collection.find({"dept_name": "Cybernetics"})
    salarios = []
    for t in instrutores:
        salarios.append(t['salary'])

    total = 0
    media = 0
    for salario in salarios:
        total += float(salario)
    media = total/len(salarios)

    print("A média dos salários dos professores de Cybernetics é: {:.2f}".format(media))

def query5():
    # 5. Recuperar o número total de créditos obtidos por um estudante específico
    collection = database['student']
    alunos = collection.find({"id": "84702"})

    total_credito = 0
    for aluno in alunos:
        total_credito += float(aluno['tot_cred'])
        aluno_nome = aluno['name']

    print(f"Total de crédito obtido do aluno {aluno_nome}:", total_credito)

def query6():
    # 6. Encontrar todas as disciplinas ministradas por um professor em um semestre específico
    semestre = "Fall"
    id_professor = "28097"

    teaches_collection = database['teaches']
    teaches_de_um_professor = teaches_collection.find({"id": id_professor}) # Todos as disciplinas ministradas por um professor específico

    info = []
    for t in teaches_de_um_professor:
        info.append({'course_id': t['course_id'], 'semester': t['semester']}) # Armazena curso e semestre ministrado por um professor

    course_collection = database['course']
    courses = course_collection.find()

    resultado_query = []
    for course in courses:
        for course_info in info:
            if course['course_id'] == course_info['course_id'] and semestre in course_info['semester']:
                resultado_query.append(course['title'])

    if(len(resultado_query) > 0):
        print(f"Curso ministrado pelo professor de id {id_professor} no semestre {semestre}:")
        for resultado in resultado_query:
            print(resultado)
    else:
        print(f"Não foi encontrado nenhum resultado para o professor de id {id_professor} e semestre {semestre}")

def query7():
    # 7. Listar todos os estudantes que têm um determinado professor como orientador
    id_prof_orientador = "19368"

    collection = database['advisor']
    advisors = collection.find({"i_id": id_prof_orientador})

    alunos = []
    for t in advisors:
        alunos.append(t['s_id'])

    if(len(alunos) > 0):
        for aluno in alunos:
            print("Aluno:", aluno)
    else:
        print(f"Não foi encontrado nenhum resultado de busca de aluno para o professor de id {id_prof_orientador}")


try:
    database = client["BancoNovo"]

    # 1. Listar todos os cursos oferecidos por um determinado departamento
    # query1()

    # 2. Recuperar todas as disciplinas de um curso específico em um determinado semestre
    collection1 = database["takes"]

    results = collection1.find({ "course_id": "169"})

    cursoESemestre = []
    for f in results:
        if(f['semester'] == "Spring"):
            cursoESemestre.append(f)

    # Imprime todas os dados em que possui o curso 169 no semestre spring
    # for t in cursoESemestre:
    #     print(t)

    # Dúvida: nesses arquivos, quais são as disciplinas?

    # 3. Encontrar todos os estudantes que estão matriculados em um curso específico
    # query3()

    # 4. Listar a média de salários de todos os professores em um determinado departamento
    # query4()

    # 5. Recuperar o número total de créditos obtidos por um estudante específico
    # query5()

    # 6. Encontrar todas as disciplinas ministradas por um professor em um semestre específico
    # query6()

    # 7. Listar todos os estudantes que têm um determinado professor como orientador
    # query7()
    client.close()

except Exception as e:
    print(e)