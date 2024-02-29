#!/usr/bin/env python
# coding: utf-8

# # Importação de Bibliotecas:

# In[1]:


import os
import pandas as pd
import pyodbc
from datetime import datetime


# ## Conexão com o banco 34:

# In[2]:


# Connection details
servidor = '****'
banco_dados = '***'
usuario = '*****'
senha = '******'

# String de conexão
conn_str = f'DRIVER=SQL Server;SERVER={servidor};DATABASE={banco_dados};UID={usuario};PWD={senha}'


# In[3]:


import os
import pyodbc
import pandas as pd
from datetime import datetime, timedelta

# String de conexão
conn_str = f'DRIVER=SQL Server;SERVER={servidor};DATABASE={banco_dados};UID={usuario};PWD={senha}'

# Configuração parquet:
chunksize = 1000000                   

# Data inicial e final desejada
data_inicial = datetime(2023, 1, 1)
data_final = datetime(2023, 12, 31)

# Loop sobre o intervalo de datas
current_date = data_inicial
while current_date <= data_final:
    # Ano e Mês:
    mes_ano = current_date.strftime("%Y_%m")

    # Ano, Mês e dia:
    dia_mes_ano = current_date.strftime("%Y_%m_%d")

    # Nome Diretório Ano e mês:
    diretorio = f'ASPECT_{mes_ano}'

    # Nome Diretório Ano, mês e dia:
    name_folder = f'ASPECT_{dia_mes_ano}'

    # Validando criação Ano e mês:
    base_path = 'E:/BACKUP_PARQUET/ASPECT'
    path_file_base = os.path.join(base_path, diretorio)
    os.makedirs(path_file_base, exist_ok=True)

    # Validando criação Ano, mês e dia:
    base_path = f'E:/BACKUP_PARQUET/ASPECT{diretorio}'
    path_file_base = os.path.join(base_path, name_folder)
    os.makedirs(path_file_base, exist_ok=True)

    # Consulta SQL:
    query = f"SELECT * FROM ASPECT_CALLDETAIL_{current_date.strftime('%Y%m')} WHERE DT_ACIONAMENTO = ?"
    
    print(query)

    # Conexão e extração:
    conn = pyodbc.connect(conn_str)
    objeto = pd.read_sql_query(query, conn, params=[current_date], chunksize=chunksize)
    

    # Caminho para salvar os arquivos parquet:
    base_path = f'E:/BACKUP_PARQUET/ASPECT{diretorio}/'

    # Início do loop para salvar os arquivos parquet:
    for i, chunk_df in enumerate(objeto):
        df_obj = pd.DataFrame(chunk_df)
        coluna = df_obj.columns

        path_file_base_idx = os.path.join(
            path_file_base,
            f"parte_{i}_.parquet"
        )
        df_obj.to_parquet(path_file_base_idx, compression='gzip', compression_level=9, engine='pyarrow')

    # Avançar para a próxima data
    conn.close()
    current_date += timedelta(days=1)
    


# In[ ]:




