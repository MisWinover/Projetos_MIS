#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import re


# In[2]:


df = pd.read_parquet('//172.21.0.15/MIS/35.Desenvolvimento/RESTORE_PARQUET/ASPECT_PARQUET_INPUT/')


# In[15]:


import re

def remove_special_characters_and_letters(input_string):
    if input_string is None:
        return None
    # Define regular expression pattern to match non-digit characters
    pattern = r'[^0-9]'
    # Use re.sub to replace non-digit characters with an empty string
    return re.sub(pattern, '', input_string)


# Apply the function to the 'CPF' column
filtered_df['CPF'] = filtered_df['CPF'].apply(remove_special_characters_and_letters)


# In[16]:


lista = ['DT_ACIONAMENTO', 'HORA', 'DT_HORA', 'SEQNUM', 'CALLID', 'SERVICE_ID', 'ID_DISPOSITION', 'USER_ID', 'CALLTYPEID', 'DIALEDNUM', 'ANI', 'SITE_ID', 'PARAM2','CPF','CLASSIFICACAO_WINOVER']

# Filtering the DataFrame based on the list of columns
filtered_df = df.loc[:, df.columns.isin(lista)]


# In[17]:


arrumar = ['CALLID', 'SERVICE_ID', 'ID_DISPOSITION','SITE_ID','CLASSIFICACAO_WINOVER']

nan_rows = filtered_df[pd.isna(filtered_df[arrumar])]


# In[18]:


# Assuming filtered_df is your DataFrame
# Assuming arrumar is your list of column names to fix

# Identify rows where values are NaN in the specified columns
nan_rows = filtered_df[filtered_df[arrumar].isna().any(axis=1)]

# Replace non-numeric values with NaN in the specified columns
filtered_df[arrumar] = filtered_df[arrumar].apply(pd.to_numeric, errors='coerce')

# Replace NaN values in the specified columns with a placeholder value (0)
filtered_df[arrumar] = filtered_df[arrumar].fillna(0)

# Convert values in the specified columns to integers
filtered_df[arrumar] = filtered_df[arrumar].astype('int64')

# Display the DataFrame with NaN rows
#print(nan_rows)

# Display the DataFrame with the specified columns converted to integers
#print(filtered_df)


# In[19]:


filtered_df.to_csv('//172.21.0.15/MIS/35.Desenvolvimento/RESTORE_PARQUET/ASPECT/filename.txt', sep='|', index=False)

