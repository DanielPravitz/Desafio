#!/usr/bin/env python
# coding: utf-8

# ### Solução utilizando PySpark

# In[105]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# In[111]:


#Import Datasets
df_contracts = spark.read.csv("contracts.csv", header=True, inferSchema=True)
df_transactions = spark.read.csv("transactions.csv", header=True, inferSchema=True)

df_contracts.show()
df_transactions = df_transactions.na.replace("null", "0")
df_transactions.show()


# In[112]:


#Calcula valor liquido da transacao 
df_liquid_value = df_transactions.selectExpr("client_id", "total_amount - (total_amount * (discount_percentage / 100)) as liquid_value")
df_liquid_value.show()


# In[113]:


#Total de ganhos
df_liquid_value.join(df_contracts, df_liquid_value["client_id"] == df_contracts["client_id"]).filter("is_active = 'true'").selectExpr("sum(liquid_value * (percentage / 100)) as total").show()

