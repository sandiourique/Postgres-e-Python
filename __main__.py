import pandas as pd
import psycopg2
from modules.connector import interface_db
import numpy as np
import statistics as st


'''
    Tratamento de dados com Pandas:
    read - ler o arquivo
    drop - excluir uma coluna que seleciono no axis - utilizo o inplace para salavar a alteração
    dropna - utilizo para excluir as linhas nulas (sem informações)
    pd.concat - utilizo o pandas para concatenar as duas planilhas
    astype - para modificar o tipo da variável que antes era object
    strftime - para modificar o formato da data.    
'''
# Editando os arquivos CSV
data_a = pd.read_csv("DADOS_1.csv", sep=",")
data_b = pd.read_csv("DADOS_2.csv", sep=",")
data_a.drop(['id'], axis=1, inplace=True)
data_b.drop(['id'], axis=1, inplace=True)
new_dataa = data_a.dropna()
new_datab = data_b.dropna()
new_data = pd.concat([new_dataa, new_datab])
new_data['data'] = new_data['data'].astype('datetime64')
new_data['data'] = new_data['data'].dt.strftime('%d/%m/%Y')

# print(new_data)


if __name__ == "__main__":
    
    interface_banco = interface_db("postgres", "311219", "127.0.0.1", "atividadetreze")
    
    # Faço um for para inserir os dados que tratei para a o banco na tabela vendas.
    try:
        for index, row in new_data.iterrows():
            interface_db.to_execute(f"INSERT INTO vendas (data, valor) VALUES ('{row['data']}', {int(row['valor'])})")
    except Exception as e:
        print(str(e)) 
    
    # Faço uma função para fazer a repetição de 50 em 50 utilizando generator como retorno.
    # Com ele consigo retornar um valor e guarda o lugar onde parou, quando executado novamente ele continua daquele ponto.
    
    def fiftyten(data):
        for i in range(0, len(data), 50): 
            yield data[i:i + 50]
    

    '''
        Inserindo os dados na tabela estatisticas:
        query - buscar os dados da tabela vendas
        data - nome da variável que guardei
        sort-values - ordenar por data
        astype - mudar o tipo da data para datetime64 e os valores para float
        exec-fiftyten - variavel no qual vou executar a função com laço de repetição for
        mean - calculo da media com a biblioteca Pandas
        median - calculo da mediana biblioteca Pandas
        mode = calculo da moda biblioteca Estatitics
        std_p = calculo do desvio_padrao biblioteca Pandas
        max_values = calculo do maior valor com a biblioteca Pandas
        min_values = calculo do menor valor com a biblioteca Pandas
        date_min = calculo da menor data no intervalo com a biblioteca Pandas
        date_max = calculo da maior data no intervalo data no intervalo com a biblioteca Pandas
    '''

    try:
        query = "SELECT * FROM vendas;"
        data = interface_db.search(query)
        data = pd.DataFrame(data)
        data = data.sort_values(by = 1, ascending=True)
        data[1] = data[1].astype('datetime64')
        data[2] = data[2].astype('float')
        exec_fiftyten = fiftyten(data)
        for data in exec_fiftyten :
            mean= data[2].mean()
            median =  data[2].median()
            mode = st.mode(data[2])
            std_p = data[2].std()
            max_values = data[2].max() 
            min_values = data[2].min()
            date_min = data[1].min()
            date_max = data[1].max()   
              
            query = f"INSERT INTO estatisticas \
                (media, mediana, moda, desvio_padrao, maior, menor, data_inicio, data_fim) \
                VALUES \
                ('{mean}', '{median}', '{mode}', '{std_p}', '{max_values}', '{min_values}','{date_min}', '{date_max}');"
            interface_db.to_execute(query)
            
    except Exception as e:
        print(str(e))
    
    