#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import mysql.connector


# In[ ]:


# Load data into a Pandas DataFrame
data = pd.read_csv(r"data/database.csv")

# dataset schema 
data.info()


# In[ ]:


data.head()


# In[ ]:


# check null values
data.isnull().sum()


# In[ ]:


# # fill null values by nothing
data = data.fillna('')


# In[ ]:


# Connect to the MySQL database
connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='earthquake_data'
)

# Create a cursor object
cursor = connection.cursor()

# Insert data into the neic_earthquakes table
for i, row in data.iterrows():
    cursor.execute("""
        INSERT INTO neic_earthquakes VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))

# Commit the changes and close the connection
connection.commit()
connection.close()


# In[ ]:





# In[ ]:




