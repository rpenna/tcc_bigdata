import os

path_local = os.path.dirname(os.path.realpath(__file__))
path_csv = os.path.join(path_local, "resultados/tempo_por_loop20181120_074330.csv")
resultado = ""
with open(path_csv, "r") as arq_csv:
    resultado = "{}".format(arq_csv.readline())
    acabou = False
    while not acabou:
        total_mysql = 0
        total_mongo = 0
        ultima_linha = 0
        linhas = []
        for _ in range(0, 75):
            linhas.append(arq_csv.readline())
        print(linhas)
        for linha in linhas:
            valores = linha.split(',')
            if valores[0] != '':
                ultima_linha = valores[0]
                total_mysql += float(valores[1])
                total_mongo += float(valores[2])
            else:
                acabou = True
        resultado += "{},{},{}\n".format(ultima_linha, total_mysql, total_mongo)
with open(os.path.join(path_local, "resultados/loop100000.csv"), 'w+') as arq_final:
    arq_final.write(resultado)
