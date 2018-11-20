""" Testes a serem feitos:
    1) Cidades cadastradas
    2) Cidades com mais beneficiados
    3) Cidades com o maior valor pago
    4) Valor total pago registrado (soma de todos os resgistros)
    5) Estados com mais beneficiados
    6) Estados com o maior valor pago
    7) Beneficiado que mais recebeu
    8) Valor pago por mês de competência"""
import os
import mysql.connector
import pprint

from pymongo import MongoClient
from datetime import datetime, timedelta
from json import dumps


def query_mysql(cursor, query, parametros=None):
    "Faz uma consulta ao banco de dados"
    cursor.execute(query, parametros)
    return cursor.fetchall()


def realizar_pesquisa(query, cursor, operacao_mongo, parametros=None, field_mongo=None):
    """Realiza a pesquisa de acordo com os argumentos passados 
    e retorna o tempo de duração e o resultado do MySQL e do MongoDB"""
    inicio = datetime.now()
    resultado_mysql = query_mysql(cursor, query, parametros)
    tempo_mysql = (datetime.now() - inicio).total_seconds()
    inicio = datetime.now()
    resultado_mongo = operacao_mongo(field_mongo)
    tempo_mongo = (datetime.now() - inicio).total_seconds()
    return {
        "mysql": {
            "tempo": tempo_mysql,
            "resultado": resultado_mysql
        },
        "mongo": {
            "tempo": tempo_mongo,
            "resultado": list(resultado_mongo)
        }
    }


def listar_cidades(cursor, colecao):
    """Realiza o teste para a listagem de cidades onde houveram moradores
    sendo beneficiados pelo bolsa família"""
    query = """SELECT DISTINCT id_cidade, no_cidade
                FROM localidade
                ORDER BY id_cidade"""
    field_mongo = [
        {"$group": {"_id": "$localidade.id_cidade", "no_cidade": {"$first": "$localidade.no_cidade"}}},
        {"$sort":{"_id": 1}}
    ]
    return realizar_pesquisa(query, cursor, colecao.aggregate, field_mongo=field_mongo)


def main():
    # conexao dos bancos de dados
    con = mysql.connector.Connect(user="root", password='', host="127.0.0.1", database="bd_bolsa_familia")
    cursor = con.cursor(dictionary=True)
    
    client = MongoClient("localhost", 27017)
    db = client.bd_bolsa_familia

    lista_cidades = listar_cidades(cursor, db.beneficios)

    path_local = os.path.dirname(os.path.realpath(__file__))
    path_json = os.path.join(path_local, "resultados/resultado_analise.json" )
    with open(path_json, "w+") as saida:
        saida.write(dumps(lista_cidades))
    

if __name__ == "__main__":
    main()