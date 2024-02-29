#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd


# # IMPORTACAO PARQUET PARA REINSERCAO OU ANALISE

# In[2]:


df = pd.read_parquet('//172.21.0.15/MIS/35.Desenvolvimento/RESTORE_PARQUET/OLOS_PARQUET_INPUT/')


# In[3]:


# Your list of columns
lista = ['Data', 'Hora', 'CallId', 'CampaignId', 'DispositionId', 'CallStart', 'CallEnd', 'PhoneNumber', 'ANI', 'Route', 'AgentId', 'DOC', 'tipo_oco']

# Filtering the DataFrame based on the list of columns
filtered_df = df.loc[:, df.columns.isin(lista)]



# In[4]:


# Assuming filtered_df is your DataFrame

# Identify rows where 'AgentId' is NaN
nan_rows = filtered_df[pd.isna(filtered_df['AgentId'])]

# Replace NaN values in 'AgentId' column with a placeholder value (-1)
filtered_df['AgentId'].fillna(0, inplace=True)

# Convert 'AgentId' column to integers
filtered_df['AgentId'] = filtered_df['AgentId'].astype(int)

# Display the DataFrame with NaN rows
#nan_rows

# Display the DataFrame with 'AgentId' column converted to integers
filtered_df


# In[11]:


# Provide a filename along with the directory path
filtered_df.to_csv('//172.21.0.15/MIS/35.Desenvolvimento/RESTORE_PARQUET/OLOS/filename.txt', sep='|', index=False)

