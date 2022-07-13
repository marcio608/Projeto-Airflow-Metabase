# Módulo para a conexão entre o python e o mysql

# Importações

import mysql.connector as mysql

# Criando a conexão

print('\nCriando a conexão com o SGBD MySQL...')

conn = mysql.connect(host = 'localhost', user = 'projeto', password = 'mor4527ujX$', database = 'DW_MYSQL')

# Abrindo um cursor

print('\nConexão feita com sucesso!')

cursor = conn.cursor()

# Criando as tabelas e as dimensões

print('\nCriando as tabelas de dimensão...')

# Primeira dimensão: range de preço (pricerange) e posição no ranking (rankingposition)

cursor.execute("CREATE TABLE IF NOT EXISTS tb_prices_rang(id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,"
"pricerange varchar(30), rankingposition INT)")


# Segunda dimensão: dimensão com prêmios (awards) e avaliação do usuário (rating)

cursor.execute("CREATE TABLE IF NOT EXISTS tb_hotels(id INT PRIMARY KEY NOT NULL AUTO_INCREMENT, "
            "name varchar(100), awards INT, rating DOUBLE)")


# Terceira dimensão: número de avaliações (numberofreview), classe do hotel (hotelclass) e avaliação do usuário (rating)

cursor.execute("CREATE TABLE IF NOT EXISTS tb_hotel_reviews(id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,"
            "numberofreview INT, hotelclass DOUBLE, rating DOUBLE)")


# Tabela fato: chaves estrangeiras e os fatos
print('\nCriando a tabela fato...')

cursor.execute("CREATE TABLE IF NOT EXISTS tb_fact(pricesrange_id INT, hotels_id INT, reviews_id INT, "
               "number_of_hotels INT, number_ofrange_prices INT, max_hotelclass INT, min_hotelclass INT, "
               "max_ofreviews INT, min_ofreviews INT, KEY pricesrange_id (pricesrange_id), KEY hotels_id (hotels_id), "
               "KEY reviews_id (reviews_id), CONSTRAINT fact_ibfk_1 FOREIGN KEY (pricesrange_id) REFERENCES "
               "tb_prices_rang (id), CONSTRAINT fact_ibfk_2 FOREIGN KEY (hotels_id) REFERENCES tb_hotels (id), "
               "CONSTRAINT fact_ibfk_3 FOREIGN KEY (reviews_id) REFERENCES tb_hotel_reviews (id))")


print("\nEstrutura do Data Warehouse criada com sucesso!\n")




