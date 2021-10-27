#!/usr/bin/env python
# coding: utf-8

# In[38]:


import pandas as pd

df_contracts = pd.read_csv("contracts.csv", header=0)
df_transactions = pd.read_csv("transactions.csv", header=0)

print(df_contracts.head(10),"\n")
print(df_transactions.head(10))

df_transactions.fillna(0, inplace=True)


# In[ ]:





# In[39]:


def calculate_liquid_value(amount, percentage):
    return amount - (amount * (percentage / 100)) 
    
df_transactions['liquid_value'] = df_transactions.apply(lambda row : calculate_liquid_value( row['total_amount'], row['discount_percentage']), axis = 1)
df_transactions = df_transactions[["client_id", "liquid_value"]]
df_transactions


# In[51]:


df = pd.merge(df_transactions, df_contracts, on=['client_id','client_id']).query("is_active != False ").eval("liquid_value * (percentage/100)")
total = sum(df)
total

