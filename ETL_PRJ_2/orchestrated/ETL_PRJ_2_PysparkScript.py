#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import glob
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ETL_With_Pyspark_Airflow") \
    .getOrCreate()


# In[ ]:


DF_Extract = spark.read \
    .format("csv") \
    .option("inferschema","true") \
    .option("header","true") \
    .load("/home/airflowsup/ETL_PRJ_2/orchestrated/airflow-extract-data_2.csv")

# DF_Extract.show()


# In[ ]:


from pyspark.sql import functions as F
from pyspark.sql import Window as W

SectorWindow = W.partitionBy(["Date added","GICS Sector"])
Output_Columns = ["Date added","GICS Sector","Aggregated_Count"]

DF_Transform = DF_Extract.drop_duplicates()
DF_Transform = DF_Transform.withColumn("Aggregated_Count", F.count(F.col("Symbol")).over(SectorWindow)) \
                          .select([F.col(f"{i}") for i in Output_Columns]).drop_duplicates()     

# DF_Transform.count()

DF_Transform.coalesce(1).write \
    .format("csv") \
    .mode("overwrite") \
    .option("header","true") \
    .save("/home/airflowsup/ETL_PRJ_2/orchestrated/airflow-transform-data_2")


    # print(csv_file)
# In[23]:


# get_ipython().system('jupyter nbconvert --to script /home/airflowsup/ETL_PRJ_2/orchestrated/ETL_PRJ_2_PysparkScript.ipynb')

