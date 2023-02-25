# Command Dev : spark2-submit --master local[*] --conf spark.key1=dataDeletion_txn_v2.csv --files dataDeletion_txn_v2.csv dataDeletion_txn_v2
# Command Dev : spark2-submit --master yarn --deploy-mode client --conf spark.key1=dataDeletion_txn_v2.csv --files dataDeletion_txn_v2.csv dataDeletion_txn_v2.py file://dataDeletion_txn_v2.csv
# Command Prod : spark2-submit --master yarn --deploy-mode cluster --conf spark.key1=dataDeletion_txn_v2.csv --files dataDeletion_txn_v2.csv dataDeletion_txn_v2.py file://dataDeletion_txn_v2.csv

# File Name         :   dataDeletion_txn_v2
# Created Date      :   17th June 2019
# Author            :   Satyaki.Basu@barclays.com
# Description       :   This is a spark script in python. This script will perform the data deletions to the input provided in dataDeletion_txn_v2.csv. This script will work for
#                       transactional & type 2 tables partitioned by dw_bus_date and active resp. The .csv file supports multiple database and table with different PK's. The script
#                       collates all the records by grouping the database and tables and then applies the deletion to the overall spark dataframe. At the end, it saves the files into
#                       a new directory partioned by dw_bus_date/active. The script reads the whole table into spark dataframe and then writes the whole into hdfs partition as a parquet format thereby
#                       ensuring the distribution of data is correct.

import pandas as pd
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.functions import col, expr, when, lit, concat, date_add, current_date, date_format,broadcast
import time
import sys


start_time = time.time()

# Set the spark session and conf parameters
spark = SparkSession.builder.appName("Data Deletion Spark Main App").getOrCreate()
spark.conf.set("spark.debug.maxToStringFields", 100)
spark.sparkContext.setLogLevel("ERROR")
filename = spark.conf.get("spark.key1")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1*1024*1024*1024)



# Step 1: Read the data correction sheet and put into a spark dataframe
pandas_df = pd.read_csv(filename)
print("Total No of data deletions :", pandas_df.shape[0],"\n")

# Step 1: Retrieve and store the database,table and PK names
grouped = pandas_df.groupby(['DATABASE_NAME','TABLE_NAME', 'PK','PARTITION_COL'])
for name,group in grouped:
    del_df = spark.createDataFrame(group)
    (g_DATABASE_NAME,g_TABLE_NAME, g_PK,g_PARTITION_COL) = name
    print("Starting data deletion for :",g_DATABASE_NAME, g_TABLE_NAME, " for ",group.shape[0],"Primary keys")

    second_time = time.time()

    # Step 2: Get the table into the dataframe.
    m_sql = 'select * from '+g_DATABASE_NAME+'.'+g_TABLE_NAME
    s_sql = 'describe formatted '+g_DATABASE_NAME+'.'+g_TABLE_NAME
    df = spark.sql(m_sql)
    print("\tTotal Records before deletion: ", df.count())

    # Step 3: Get the hive table file location
    schema = spark.sql(s_sql).filter('col_name = "Location:"').select('data_type')
    location = schema.collect()
    hive_table_location = location[0].data_type


    # Step 4: Create a sorted list of columns for the union to work properly. Take any dataframe to get the schema
    col_list = df.schema.names
    s_list = [i for i in col_list]


    # Step 5: Join the 2 dataframes using a left outer condition
    cond = [df[g_PK] == del_df['PK_VALUE']]
    no_recs_to_delete = df.join(broadcast(del_df), cond, "inner").count()
    print("\tNo of records found for deletion: ", no_recs_to_delete)

    final_df = df.join(broadcast(del_df), cond, "leftanti").select(s_list)
    print("\tTotal Records after deletion: ",final_df.count())

    # Step 6: Write this to a new location
    third_time = time.time()
    partition_directory=hive_table_location+'/GRMS/'
    final_df.write.format('parquet').mode("overwrite").partitionBy(g_PARTITION_COL).save(partition_directory)
    print("\tWriting of partitions completed.....in...", time.time() - third_time, " seconds.....in.....",partition_directory,"\n")

# Step 7: Stop spark
print("Overall time taken..",time.time()- start_time," seconds\n")
spark.stop()

