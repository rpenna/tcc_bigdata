# -*- coding: utf-8 -*-
""" Testes a serem feitos:
    1) Cidades cadastradas - FEITO
    2) Cidades com mais beneficiados - FEITO
    3) Cidades com o maior valor pago - FEITO
    4) Valor total pago registrado (soma de todos os resgistros) - FEITO
    5) Beneficiado que mais recebeu - FEITO
    6) Valor pago por mês de competência - FEITO"""
import os
import mysql.connector
import pprint

from pymongo import MongoClient
from datetime import datetime, timedelta
from json import dumps


def query_mysql(cursor, query, parametros=None):
    "Faz uma consulta ao banco de dados"
    try:
        cursor.execute(query, parametros)
        return cursor.fetchall()
    except mysql.connector.errors.OperationalError:
        return {
            "timeout": "True"
        }


def realizar_pesquisa(descricao, query, cursor, operacao_mongo, parametros=None, field_mongo=None):
    """Realiza a pesquisa de acordo com os argumentos passados 
    e retorna o tempo de duração e o resultado do MySQL e do MongoDB"""
    print("Processando MySQL...")
    inicio = datetime.now()
    resultado_mysql = query_mysql(cursor, query, parametros)
    tempo_mysql = (datetime.now() - inicio).total_seconds()
    print("Processando Mongo...")
    inicio = datetime.now()
    resultado_mongo = operacao_mongo(field_mongo, allowDiskUse=True)
    tempo_mongo = (datetime.now() - inicio).total_seconds()
    return {
        "descricao": descricao,
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
    descricao = "Realiza o teste para a listagem de cidades onde houveram moradores sendo beneficiados pelo bolsa família"
    query = """SELECT DISTINCT id_cidade, no_cidade
                FROM localidade
                ORDER BY id_cidade"""
    field_mongo = [
        {"$group": {"_id": "$localidade.id_cidade", "no_cidade": {"$first": "$localidade.no_cidade"}}},
        {"$sort":{"_id": 1}}
    ]
    return realizar_pesquisa(descricao, query, cursor, colecao.aggregate, field_mongo=field_mongo)


def listar_cidades_com_mais_beneficiados(cursor, colecao):
    descricao = "Realiza o teste para a listagem de cidades ordenadas de forma decrescente por quantidade de beneficiados"
    query = """SELECT l.id_cidade, l.no_cidade, count(*) AS qtd
                FROM localidade l JOIN beneficiario b ON l.id_cidade = b.id_cidade
                GROUP BY l.id_cidade
                ORDER BY qtd DESC;"""
    field_mongo = [
        {"$group": {"_id": "$localidade.id_cidade", 
                    "no_cidade": {"$first": "$localidade.no_cidade"},
                    "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    return realizar_pesquisa(descricao, query, cursor, colecao.aggregate, field_mongo=field_mongo)


def listar_cidades_com_maior_valor_pago(cursor, colecao):
    descricao = "Realiza o teste para a listagem de cidades ordenadas de forma decrescente por valor pago"
    query = """SELECT l.id_cidade, l.no_cidade, sum(p.vl_beneficio) AS valor
                FROM localidade l JOIN beneficiario b ON l.id_cidade = b.id_cidade
                    JOIN pagamento p ON p.nu_nis = b.nu_nis
                GROUP BY l.id_cidade
                ORDER BY valor DESC;"""
    field_mongo = [
        {"$group": {"_id": "$localidade.id_cidade", 
                    "no_cidade": {"$first": "$localidade.no_cidade"},
                    "valor": {"$sum": {"$sum": "$pagamentos.vl_beneficio"}}}},
        {"$sort": {"valor": -1}}
    ]
    return realizar_pesquisa(descricao, query, cursor, colecao.aggregate, field_mongo=field_mongo)


def calcular_valor_total_pago(cursor, colecao):
    descricao = "Calcula o valor somado de todos os benefícios pagos"
    query = """SELECT sum(vl_beneficio)
                FROM pagamento
                """
    field_mongo = [
        {
            "$group": {
                "_id": None,
                "total": {
                    "$sum": {
                        "$sum": "$pagamentos.vl_beneficio"
                    }
                }
            }
        }
    ]
    return realizar_pesquisa(descricao, query, cursor, colecao.aggregate, field_mongo=field_mongo)


def buscar_maior_beneficiado(cursor, colecao):
    descricao = "Retorna os dados com o NIS que recebeu mais benefícios"
    query = """SELECT b.nu_nis, b.no_beneficiario, l.no_cidade, l.no_uf, maior.valor
                FROM
                    (SELECT MAX(total.beneficio) AS valor
                        FROM (SELECT nu_nis, sum(vl_beneficio) AS beneficio
                            FROM pagamento
                            GROUP BY nu_nis) total
                        ) maior,
                    beneficiario b,
                    localidade l,
                    (SELECT nu_nis, sum(vl_beneficio) AS beneficio
                            FROM pagamento
                            GROUP BY nu_nis) valores
                    WHERE b.nu_nis = valores.nu_nis
                        AND valores.beneficio = maior.valor
                        AND b.id_cidade = l .id_cidade;"""
    field_mongo = [
        {
            "$group": {
                "_id": "$beneficiario.nu_nis",
                "valor": {
                    "$max": {
                            "$sum": "$pagamentos.vl_beneficio"
                    }
                },
                "infos": {
                    "$push": {
                        "nome": "$beneficiario.no_beneficiario",
                        "cidade": "$localidade.no_cidade",
                        "estado": "$localidade.no_uf"
                    }
                }
            }
        },
        {
            "$sort": {
                "valor": -1
            }
        },
        {
            "$limit": 1
        }
    ]
    return realizar_pesquisa(descricao, query, cursor, colecao.aggregate, field_mongo=field_mongo)


def buscar_valor_por_mes_competencia(cursor, colecao):
    descricao = "Retorna o valor total pago em beneficios por cada mes de competência"
    query = """SELECT dt_competencia, SUM(vl_beneficio) AS total
                FROM pagamento
                GROUP BY dt_competencia;"""
    field_mongo = [
        {
            "$unwind": "$pagamentos"
        },
        {
            "$group": {
                "_id": "$pagamentos.dt_competencia",
                "total": {
                    "$sum": "$pagamentos.vl_beneficio"
                }
            }
        }
    ]
    return realizar_pesquisa(descricao, query, cursor, colecao.aggregate, field_mongo=field_mongo)


def main():
    # conexao dos bancos de dados
    con = mysql.connector.Connect(user="root", password='', host="127.0.0.1", database="bd_bolsa_familia", connection_timeout=1800)
    cursor = con.cursor(dictionary=True)
    
    client = MongoClient("localhost", 27017)
    db = client.bd_bolsa_familia

    resultados =  []
    #resultados.append(listar_cidades(cursor, db.beneficios))
    #resultados.append(listar_cidades_com_mais_beneficiados(cursor, db.beneficios))
    #resultados.append(listar_cidades_com_maior_valor_pago(cursor, db.beneficios))
    #resultados.append(calcular_valor_total_pago(cursor, db.beneficios))
    #resultados.append(buscar_maior_beneficiado(cursor, db.beneficios))
    resultados.append(buscar_valor_por_mes_competencia(cursor, db.beneficios))

    path_local = os.path.dirname(os.path.realpath(__file__))
    path_json = os.path.join(path_local, "resultados/resultado_analise.json" )
    with open(path_json, "w+") as saida:
        saida.write(dumps(resultados, indent=4))
    

if __name__ == "__main__":
    main()