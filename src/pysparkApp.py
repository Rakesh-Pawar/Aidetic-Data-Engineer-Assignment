#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import findspark

# Set Java and Spark paths
os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-21"
os.environ["SPARK_HOME"] = "C:\pyspark\spark-3.5.0-bin-hadoop3"

# Initialize findspark
findspark.init()


# In[ ]:


# mysql_db_driver_class = "com.mysql.jdbc.Driver"
# table_name = "neic_earthquakes"
# host_name = "localhost"
# port_no = "3306"
# user_name = "root"
# password = "root"
# database_name = "earthquake_data"


# In[ ]:


from pyspark.sql import SparkSession

# Set the path to the MySQL Connector/J JAR file
mysql_connector_jar = "mysql-connector-j-8.0.33.jar"

# Create a Spark session
spark = SparkSession.builder \
    .appName("Aidetic") \
    .config("spark.jars", mysql_connector_jar) \
    .getOrCreate()

# MySQL connection 
mysql_url = "jdbc:mysql://127.0.0.1:3306/earthquake_data"
mysql_properties = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.jdbc.Driver"
}

# Read data from MySQL table into PySpark DataFrame
initialDF = spark.read.format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", "neic_earthquakes") \
    .option("user", mysql_properties["user"]) \
    .option("password", mysql_properties["password"]) \
    .option("driver", mysql_properties["driver"]).load()

# Show the DataFrame
initialDF.show(n=5, truncate=False)




# In[ ]:


# display top 5 records in pandas 
initialDF.toPandas().head()


# In[ ]:


initialDF.count()


# In[ ]:


type(initialDF)


# In[ ]:


initialDF.printSchema()


# In[ ]:


df = initialDF


# ## How does the Day of a Week affect the number of earthquakes?
# 

# In[ ]:


# import packages
from pyspark.sql.functions import *
from pyspark.sql.types import *


# In[ ]:


df = df.withColumn("Date", to_date("date", "dd/MM/yyyy"))
df.show(5)


# In[ ]:


# df = df.withColumn('DateOfWeek', dayofweek('date'))


# In[ ]:


import pyspark.sql.functions as f

# count of week days  
result_df = (
     df.withColumn('Day', date_format('date', 'E'))
    .groupBy('Day')
    .count()
    .na.drop()
)

# Show the result
result_df.show()


# In[ ]:


result_df.toPandas().set_index('Day').plot(kind='bar')


# ## What is the relation between Day of the month and Number of earthquakes that happened in a year?

# In[ ]:


# Extract day of month and year from the 'Date' column
df = df.withColumn('DayOfMonth', dayofmonth('Date')).withColumn('Year', year('Date'))

# Group by year and day of month, then count the number of earthquakes
result_df2 = df.groupBy('Year', 'DayOfMonth').count().na.drop().orderBy("Year", "DayOfMonth")

# Show the result
result_df2.show(10)


# In[ ]:


from pyspark.sql.functions import corr

# Calculate correlation between DayOfMonth and count
correlation = result_df2.stat.corr('DayOfMonth', 'count')

print(f"Correlation between DayOfMonth and count: {correlation}")


# #### correlation coefficient of -0.01 suggests a very weak negative correlation between the day of the month and the number of earthquakes.

# ## What does the average frequency of earthquakes in a month from the year 1965 to 2016  tell us?
# 

# In[ ]:


# Extract month and year from the 'Date' column
df = df.withColumn('Year', year("Date")).withColumn("Month", month("Date"))

# filter Data from year 1965 to 2016
filter_df = df.filter((col('Year')>=1965) & (col("Year")<=2016))

# frequency of earthquakes by year and month 
freq_by_month = filter_df.groupBy('Year', 'Month').agg(count('*').alias('Frequency'))

# average frequency
avg_freq_by_month = (
     freq_by_month
    .groupBy()
    .agg(mean('Frequency').alias('Avg_Frequency')))

# Show the result
avg_freq_by_month.show()


# ## What is the relation between Year and Number of earthquakes that happened in that year?

# In[ ]:


# Group by year & count the number of earthquakes
total_earthquakes = df.groupBy('Year').count().na.drop().orderBy("Year")

# Show the result
total_earthquakes.show(10)


# In[ ]:


import matplotlib.pyplot as plt

# Group by year & count the number of earthquakes
total_earthquakes = df.groupBy('Year').count().na.drop().orderBy("count", ascending=False).limit(10)

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = total_earthquakes.toPandas()

# Set the "Year" column as the index
pandas_df = pandas_df.set_index("Year")

# Plot the top 10 years with the highest counts
plt.figure(figsize=(12, 15))
pandas_df.plot(kind='bar', legend=False)  # Assuming you don't want to show the legend
plt.title('Top 10 Years with the Highest Number of Earthquakes')
plt.xlabel('Year')
plt.ylabel('Number of Earthquakes')
plt.show()


# #### In 2011 higest number of earthquakes happened 

# ## How has the earthquake magnitude on average been varied over the years?
# 

# In[ ]:


# Group by year and calculate the average magnitude for each year
average_magnitude_by_year = df.groupBy('Year').agg(avg('Magnitude').alias('AverageMagnitude'))

# Show the result
average_magnitude_by_year.show()


# ## How does year impact the standard deviation of the earthquakes?

# In[ ]:


# calculate the standard deviation of earthquake magnitudes for each year
magnitude_std_by_year = df.groupBy('Year').agg(stddev('Magnitude').alias('MagnitudeStdDev')).na.drop()

# Show the result
magnitude_std_by_year.show()


# ## Does geographic location have anything to do with earthquakes?
# 
# Yes, the geographic location is a critical factor in understanding earthquakes. We can plot a world map with the help of geographic location. We can predict in which location the earthquake will happen. I am plotting a simple world map that shows the magnitude of earthquakes as per location.

# In[ ]:


df.groupBy("Longitude","Latitude").count().orderBy("count", ascending=False).limit(5).show()


# In[ ]:


df = df.withColumn("Magnitude", df["Magnitude"].cast("double"))\
    .withColumn("Longitude", df["Longitude"].cast("double"))\
    .withColumn("Latitude", df["Latitude"].cast("double"))

# Plot the scatter plot
df.toPandas().plot(x="Longitude", y="Latitude", kind="scatter", c="Magnitude", colormap="YlOrRd", figsize=(15, 10))
plt.title('Earthquake Magnitudes by Location')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.show()


# ## Where do earthquakes occur very frequently?

# In[ ]:


from pyspark.sql.functions import count, col, desc

columns = ['Latitude', 'Longitude', 'Magnitude']

# Group by 'Latitude' and 'Longitude', then aggregate sum of 'Magnitude'
sorted_earthquake_data = df.select(columns).groupBy('Latitude', 'Longitude')\
                           .agg(count('Magnitude').alias('TotalMagnitude')).na.drop()\
                           .orderBy(col("TotalMagnitude").desc()).limit(1)

# Show the sorted DataFrame
sorted_earthquake_data.show()


# ## What is the relation between Magnitude, Magnitude Type , Status and Root Mean Square of the earthquakes?
# 

# In[ ]:


df1 = df.select("Magnitude","Magnitude_type","Root_Mean_Square","Status")
df1.printSchema()


# In[ ]:


df1 = df1.withColumn("Magnitude", df1["Magnitude"].cast("double"))
df1 = df1.withColumn("Root_Mean_Square",df1["Root_Mean_Square"].cast("double"))
df1.printSchema()


# In[ ]:


df1 = df1.withColumn("Root_Mean_Square", when(col("Root_Mean_Square")=="", np.nan).otherwise(col("Root_Mean_Square")))
df1 = df1.withColumn("Magnitude", when(col("Magnitude")=="", np.nan).otherwise(col("Magnitude")))\
          .withColumn("Magnitude_type", when(col("Magnitude_type")=="", np.nan).otherwise(col("Magnitude_type"))) \
          .withColumn("Status", when(col("Status")=="", np.nan).otherwise(col("Status")))
df1.show(10)


# In[ ]:


df1.na.drop()


# In[ ]:


pandas_df = df1.toPandas()

# Select numeric columns only
numeric_columns = pandas_df.select_dtypes(include='number')

# Compute correlation for numeric columns
correlation_matrix = numeric_columns.corr()

# Display the correlation matrix
print(correlation_matrix)


# In[ ]:




