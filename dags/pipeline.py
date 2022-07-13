# DAG

# Este pipeline carrega os dados da fonte (arquivo json), aplica as transformações, salva no
# formato CSV e insere no DW.


# Início do processo ETL


# Importações

import json
import pendulum  # para gerenciar datas em python
import pandas as pd
from airflow.models import DAG  # informa ao Airflow quais tarefas devem ser executadas.
from airflow.operators.python import PythonOperator
import conecta_db.conecta_db as conn_dw

# Caminho da pasta contendo os dados

input = '/home/mor/Desktop/Projeto_Airflow/entrada/dados_hoteis.json'

# Função que carrega o arquivo json (fonte de dados)

def carrega_arquivo():
    with open(input, 'r') as f:
        data = json.load(f)
    return data


# Função para extrair os atributos dos hotéis

def extrai_atributos(path: str):

    # Carregando o arquivo
    data = carrega_arquivo()

    #Lista para os dados que serão extraídos
    lista_dados = []

    try:
        for linha in data:

            # Extraindo as colunas desejadas e inserido na lista

            lista_dados.append({'ID': linha['id'],
                                'Name': linha['name'],
                                'Type': linha['type'],
                                'rating': linha['rating'],
                                'Awards': len(linha['awards']),
                                'RankingPosition': linha['rankingPosition'],
                                'HotelClass': linha['hotelClass'],
                                'NumberofReviews': linha['numberOfReviews'],
                                'priceRange': linha['priceRange']
                                
            })
    
    except:
        print('Ocorreu um erro ao carrgar a lista de dados!')
        pass


    # Criando um dataframe com os dados extraidos

    df1 = pd.DataFrame(lista_dados)

    # Gravando o dataframe no formato CSV
    df1.to_csv(path, index= False)



# Extraindo o preço dos hotéis e posição no ranking

def price_range_position(path: str):
    data = carrega_arquivo()

    lista_dados = []

    try:

        for linha in data:

            lista_dados.append({'priceRange': linha['priceRange'],
            'RankingPosition': linha['rankingPosition']
            
            })

    except:
        print('Ocorreu um erro ao carrgar a lista de dados dos preços e rankings!')
        pass
    df2 = pd.DataFrame(lista_dados)
    df2.to_csv(path, index = False)


# Função para extrair nome, prêmeio e avaliações do hotéis

def hotel_award_rating(path: str):

    data = carrega_arquivo()

    lista_dados = []

    try:
        for linha in data:
            lista_dados.append({'Name': linha['name'],
            'Awards': len(linha['awards']),
            'rating': linha['rating']
            
            })

    except:
        print('Ocorreu um erro ao carrgar a lista de nomes, awards e rating!')
        pass

    df3 = pd.DataFrame(lista_dados)
    df3.to_csv(path, index = False)


# Função para extrair número de avaliações, classe do hotel e avaliações do usuários

def reviews_class_rating(path: str):
    data = carrega_arquivo()

    lista_dados = []

    try:

        for linha in data:
            lista_dados.append({'NumberOfReviews': linha['numberOfReviews'],
                                'HotelClass': linha['hotelClass'],
                                'rating': linha['rating']
            })
    except:
        print("Ocorreu algum problema ao carregar a lista de dados.")
        pass

    df4 = pd.DataFrame(lista_dados)
    df4.to_csv(path, index = False)


# Função para carregar os dados nas tabelas do DW

def insert_dimensions():

    # Carregando o arquivo

    dados_dim1 = pd.read_csv('/home/mor/Desktop/Projeto_Airflow/stage/resultado_2.csv', index_col= False, delimiter= ',')
    dados_dim1.head()

    # Desativando o modo de segurança do Mysql

    sql = 'SET SQL_SAFE_UPDATES = 0;'
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Desativando a checagem de FK do Mysql

    sql = 'SET FOREIGN_KEY_CHECKS = 0;'
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Limpando a tabela antes de inserir os dados

    sql = 'TRUNCATE TABLE tb_prices_rang'
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Loop pelos dados para fazer a inserção

    # iterrow() é um método que a cada iteração produz um objto índice (i) e um objeto row (row)
    # Esse método permite iterar cada linha do dataframe.

    for i, row in dados_dim1.iterrows():
        sql = 'INSERT INTO tb_prices_rang(pricerange, rankingposition) VALUES(%s, %s)'
        conn_dw.cursor.execute(sql, tuple(row))
        conn_dw.conn.commit()
    


    dados_dim2 = pd.read_csv('/home/mor/Desktop/Projeto_Airflow/stage/resultado_3.csv', index_col= False, delimiter = ',')
    dados_dim2.head()

    sql = 'TRUNCATE TABLE tb_hotels'
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    for i, row in dados_dim2.iterrows():

        sql = 'INSERT INTO tb_hotels(name, awards, rating) VALUES (%s, %s, %s)'
        conn_dw.cursor.execute(sql, tuple(row))
        conn_dw.conn.commit()
    


    dados_dim3 = pd.read_csv('/home/mor/Desktop/Projeto_Airflow/stage/resultado_4.csv', index_col= False, delimiter = ',')
    dados_dim3.head()

    sql = 'TRUNCATE TABLE tb_hotel_reviews'
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    for i, row in dados_dim3.iterrows():
        sql = 'INSERT INTO tb_hotel_reviews(numberofreview, hotelclass, rating) VALUES (%s, %s, %s)'
        conn_dw.cursor.execute(sql, tuple(row))
        conn_dw.conn.commit()



# Inserindo os dados na tabela fato

def insert_fact():

    sql = 'TRUNCATE TABLE tb_fact'
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    sql = "INSERT INTO tb_fact(pricesrange_id, hotels_id, reviews_id, number_of_hotels, number_ofrange_prices, max_hotelclass, min_hotelclass, " \
          "max_ofreviews, min_ofreviews) select tb_prices_rang.id, tb_hotels.id, tb_hotel_reviews.id, count(name), count(pricerange), max(hotelclass), min(hotelclass), " \
          "max(numberofreview), min(numberofreview) FROM tb_prices_rang, tb_hotels, tb_hotel_reviews GROUP BY tb_prices_rang.id, tb_hotels.id, tb_hotel_reviews.id; "


    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Ativando a checagem de FK do Mysql

    sql = 'SET FOREIGN_KEY_CHECKS = 1;'
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()

    # Ativando o modo de segurança do MySQL

    sql = 'SET SQL_SAFE_UPDATES = 1'
    conn_dw.cursor.execute(sql)
    conn_dw.conn.commit()





# Execução da DAG

with DAG(dag_id = 'DAG_Projeto_Airflow', start_date = pendulum.datetime(2022, 1, 1, tz = 'UTC'), schedule_interval = '@Daily', catchup = False) as dag:
    # catchup = False significa não fazer execuções retroativas.

    # Task para a carga de dados:

    task1_carrega_dados = PythonOperator(
        task_id = 'carrega_dados',
        python_callable = carrega_arquivo,
        dag = dag
    )

    # Task de extração de atributos:

    task2_extrai_dados = PythonOperator(
        task_id = 'extrai_atributos',
        python_callable = extrai_atributos,
        op_kwargs = {'path': '/home/mor/Desktop/Projeto_Airflow/stage/resultado_1.csv'},
        dag = dag
    )

    # Task de extração do range de preço:

    task3_prices_range_position = PythonOperator(
        task_id = 'prices_range_position',
        python_callable = price_range_position,
        op_kwargs = {'path': '/home/mor/Desktop/Projeto_Airflow/stage/resultado_2.csv'},
        dag = dag
    )

    # Task de extração de dados dos hotéis:

    task4_hotel_award_rating = PythonOperator(
        task_id = 'hotel_award_rating',
        python_callable = hotel_award_rating,
        op_kwargs = {'path': '/home/mor/Desktop/Projeto_Airflow/stage/resultado_3.csv'},
        dag = dag
    )


    # Task de extração de avaliações:

    task5_reviews_class_rating = PythonOperator(
        task_id = 'reviews_class_rating',
        python_callable = reviews_class_rating,
        op_kwargs = {'path': '/home/mor/Desktop/Projeto_Airflow/stage/resultado_4.csv'},
        dag = dag
    )

    # Task de inserção de dados nas dimensões:

    task6_insert_dimensions =PythonOperator(
        task_id = 'insert_dimensions',
        python_callable = insert_dimensions,
        dag = dag

    )

    # Task de inserção de dados na tabela fato:

    task7_insert_fact = PythonOperator(
        task_id = 'insert_fact',
        python_callable = insert_fact,
        dag = dag
    )


# Ordem em que as tarefas serão executadas
# Isso sim é o grafo direcionado.


task1_carrega_dados >> task2_extrai_dados >> task3_prices_range_position >> task4_hotel_award_rating >> task5_reviews_class_rating >> task6_insert_dimensions >> task7_insert_fact


































