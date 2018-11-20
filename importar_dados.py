# -*- coding: utf-8 -*-
import os
import pprint
import mysql.connector

from pymongo import MongoClient
from datetime import datetime, timedelta


def executar_mysql(con, cursor, query, parametros=None):
    "Manipula o banco de dados MySQL"
    cursor.execute(query, parametros)
    con.commit()


def cadastrar_cidade(con, cursor, parametros):
    "Insere cidade na tabela localidade, caso ela ainda nao esteja cadastrada"
    try:
        query = "INSERT INTO localidade VALUES (%s, %s, %s)"
        executar_mysql(con, cursor, query, parametros)
    except mysql.connector.errors.IntegrityError:
        # chave duplicada, significa que ja houve o cadastro desta localidade
        pass


def cadastrar_beneficiario(con, cursor, parametros):
    "Insere beneficiario na tabela beneficiario, caso ele ainda nao esteja cadastrado"
    try:
        query = "INSERT INTO beneficiario VALUES (%s, %s, %s)"
        executar_mysql(con, cursor, query, parametros)
    except mysql.connector.errors.IntegrityError:
        # chave duplicada, significa que ja houve o cadastro deste baneficiario
        pass


def cadastrar_pagamento(con, cursor, parametros):
    try:
        query = "INSERT INTO pagamento VALUES (%s, %s, %s, %s)"
        executar_mysql(con, cursor, query, parametros)
    except mysql.connector.errors.IntegrityError:
        # chave duplicada, significa que ja houve o cadastro deste pagamento
        pass

def importar_mysql(con, cursor, dados):
    "Importa os dados para o MySQL e retorna o tempo total"
    inicio = datetime.now()
    cadastrar_cidade(con, cursor, (int(dados[3]), dados[4], dados[2]))
    cadastrar_beneficiario(con, cursor, (int(dados[5]), dados[6], int(dados[3])))
    cadastrar_pagamento(con, cursor, (int(dados[5]), dados[0][1:], dados[1], float(dados[7].replace(',', '.'))))
    fim = datetime.now()
    return (fim-inicio).total_seconds()


def importar_mongo(client, db, dados):
    "Importa os dados para o Mongo e retorna o tempo total"
    inicio = datetime.now()
    beneficiario = {"beneficiario.nu_nis": int(dados[5])}
    pagamento = {
                "dt_referencia": dados[0][1:],
                "dt_competencia": dados[1],
                "vl_beneficio": float(dados[7].replace(',', '.'))
            }
    updt = db.beneficios.update_one(beneficiario, {"$push": {"pagamentos": pagamento}})
    if updt.modified_count == 0: # se nenhum documento foi atualizado, inserir o beneficiario e sua cidade no bd
        estrutura = {
            "localidade": {
                "id_cidade": int(dados[3]),
                "no_cidade": dados[4],
                "no_uf": dados[2]
            },
            "beneficiario": {
                "nu_nis": int(dados[5]),
                "no_beneficiario": dados[6],
            },
            "pagamentos": [
                {
                    "dt_referencia": dados[0][1:],
                    "dt_competencia": dados[1],
                    "vl_beneficio": float(dados[7].replace(',', '.'))
                }
            ]
        }
        db.beneficios.insert(estrutura)
    fim = datetime.now()
    return (fim-inicio).total_seconds()


def importar_arquivo(bolsa_familia, con, cursor, client, db, total_pagtos):
    "Lê o arquivo e retorn a quantidade de linhas lidas e o tempo gasto para leitura em cada banco de dados"
    
    # inicializacao das variaveis
    dados = []
    tempo_mysql = 0
    tempo_mongo = 0
    tamanho_arquivo = 0
    dado = bolsa_familia.readline() # le a primeira linha, que contem apenas o cabecalho
    dados.append(bolsa_familia.readline()) # le a segunda linha
    tempo_acumulado = ""
    tempo_instantaneo = ""

    # processamento do arquivo a cada 1000 linhas
    while dado:
        try:
            tempo_importacao_mongo = 0
            tempo_importacao_mysql = 0
            while dado and len(dados) < 1000:
                dado = bolsa_familia.readline()
                dados.append(dado)
            tamanho_arquivo += len(dados)
            for beneficio in dados:
                beneficio = str(beneficio).replace('\"', '').replace("\\r\\n", '').replace("\'", '').split(';')
                if len(beneficio) > 1:
                    tempo_importacao_mysql += importar_mysql(con, cursor, beneficio)
                    tempo_importacao_mongo += importar_mongo(client, db, beneficio)
            tempo_mysql += tempo_importacao_mysql
            tempo_mongo += tempo_importacao_mongo
            dados = []
            print("{} linhas importadas\nTempo parcial MySQL: {} segundos\nTempo parcial MongoDB: {} segundos\n-----".format(tamanho_arquivo, tempo_mysql, tempo_mongo))
            print("{} linhas importadas\nTempo para importacao de até 1000 linhas no MySQL: {} segundos\nTempo para importacao de 1000 linhas no MongoDB: {} segundos\n-----\n".format(tamanho_arquivo, tempo_importacao_mysql, tempo_importacao_mongo))
            tempo_acumulado += "{},{},{}\n".format(tamanho_arquivo, tempo_mysql, tempo_mongo)
            tempo_instantaneo += "{},{},{}\n".format(tamanho_arquivo, tempo_importacao_mysql, tempo_importacao_mongo) 
        except KeyboardInterrupt:
            print("Excecao detectada, encerrando importacao")
            return tamanho_arquivo, tempo_mysql, tempo_mongo, tempo_acumulado, tempo_instantaneo
    return tamanho_arquivo, tempo_mysql, tempo_mongo, tempo_acumulado, tempo_instantaneo

def main():
    # localizacao dos arquivos
    path_local = os.path.dirname(os.path.realpath(__file__))
    path_csv = os.path.join(path_local, "data" )
    arquivos = os.listdir(path_csv)

    # inicializacao de variaveis
    tempo_total_mysql = 0
    tempo_total_mongo = 0
    total_pagtos = 0
    relatorio = ""
    csv_tempo_parcial = "Arquivos importados,Tempo acumulado MySQL,Tempo acumulado MongoDB\n"
    csv_tempo_por_importacao = "Arquivos importados,Tempo da importação no MySQL,Tempo da importação MongoDB\n"

    # conexao dos bancos de dados
    con = mysql.connector.Connect(user="root", password='', host="127.0.0.1", database="bd_teste")
    cursor = con.cursor(dictionary=True)
    
    client = MongoClient("localhost", 27017)
    db = client.bd_teste
    
    # importacao dos arquivos para os bancos de dados
    for arquivo in arquivos:
        if arquivo[-4:] == ".csv" and arquivo[:2] != "._":
            with open(os.path.join(path_csv, arquivo), 'rb') as bolsa_familia:
                print("Importando dados de {}...".format(arquivo))
                pagtos_arquivo, tempo_mysql, tempo_mongo, tempo_acumulado, tempo_instantaneo = importar_arquivo(bolsa_familia, con, cursor, client, db, total_pagtos)
                tempo_total_mysql += tempo_mysql
                tempo_total_mongo += tempo_mongo
                total_pagtos += pagtos_arquivo
                relatorio += "\n#####\nImportacao de {} concluida com {} pagamentos!\n-----".format(arquivo, pagtos_arquivo)
                csv_tempo_parcial += tempo_acumulado
                csv_tempo_por_importacao += tempo_instantaneo

    # geracao do relatorio do resultado final
    relatorio += "\nIMPORTAÇÃO FINALIZADA\nUm total de {} pagamentos foram identificados.\n".format(total_pagtos)
    resultado = "{}Tempo total para importacao de dados para o MySQL: {} segundos\nTempo total para importacao de dados para o MongoDB: {} segundos".format(relatorio, tempo_total_mysql, tempo_total_mongo)
    print("\n#####\n{}".format(resultado))
    with open(os.path.join(path_local, "resultados/importacao{}.txt".format(datetime.now().strftime("%Y%m%d_%H%M%S"))), "w+") as saida:
        saida.write(resultado)
    with open(os.path.join(path_local, "resultados/tempo_total{}.csv".format(datetime.now().strftime("%Y%m%d_%H%M%S"))), "w+") as saida:
        saida.write(csv_tempo_parcial)
    with open(os.path.join(path_local, "resultados/tempo_por_loop{}.csv".format(datetime.now().strftime("%Y%m%d_%H%M%S"))), "w+") as saida:
        saida.write(csv_tempo_por_importacao)
    con.close()


if __name__ == "__main__":
    main()
