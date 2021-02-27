import os
import pandas as pd
import sqlalchemy

str_connection = 'sqlite:///{path}' # str de conexão com o bando local

# Os enderços de nosso projeto e subpasta
BASE_DIR = os.path.abspath('.')  
DATA_DIR = os.path.join( BASE_DIR, 'data' ) #É a maneira mais segura de somar BASE_DIR com data, independendo do sistema operacional

'''# Forma 1
files_names = os.listdir( DATA_DIR )
correct_files = []
for i in files_names:
    if i.endswith('.csv'):
        correct_files.append(i)

for i in correct_files:
    print(i)
'''
# Forma 2 (usando compressão)
# Encontrnando arquivos de dados
files_names = [i for i in os.listdir( DATA_DIR ) if i.endswith('.csv')]

# Abrindo conexão com banco

str_connection = str_connection.format( path=os.path.join( DATA_DIR, 'olist.db' ) ) # Conexão com o banco local

# Para cada arquivo é realizado uma inserção no banco 
for i in files_names:
    print(i)
    df_tmp = pd.read_csv( os.path.join( DATA_DIR, i ) )
    table_name = 'tb_' + i.strip( '.csv').replace('olist_', "").replace('_dataset', '') # tirando 'isso' do nome dos arquivos da pasta data
    df_tmp.to_sql(  table_name, 
                    connection, 
                    if_exists='replace',
                    index=False )
