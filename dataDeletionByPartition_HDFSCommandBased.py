'''
PK,PK_VALUE,DATABASE_NAME,TABLE_NAME,PARTITION_COL
STG_ID,300010000011393,e_ukba_db,tehcent_satyaki,dw_bus_dt
STG_ID,300010001097885,e_ukba_db,tehcent_satyaki,dw_bus_dt
CUS_IDR,1000000004,E_CUSTOMER_DB,TDLPCUS_satyaki,active
'''

# Command Dev :  spark2-submit --master local[*] --conf spark.key1=dataDeletionPartition.csv --files dataDeletion_v4.csv dataDeletionByPartition_v3.py
# Command Dev :  spark2-submit --master yarn --deploy-mode client --conf spark.key1=dataDeletionPartition.csv --files dataDeletion_v4.csv dataDeletionByPartition_v3.py file://dataDeletionPartition.csv
# Command Prod : spark2-submit --master yarn --deploy-mode cluster --conf spark.key1=dataDeletionPartition.csv --files dataDeletion_txn_v2.csv dataDeletionByPartition_v3.py file://dataDeletionPartition.csv

# File Name         :   dataDeletionByPartition_v3.py
# Created Date      :   17th June 2019
# Author            :   Satyaki.Basu@barclays.com
# Description       :   This is a spark script in python. This script will perform the data deletions to the input provided in dataDeletion_v4.csv. This script will work for
#                       transactional & type 2 tables partitioned by dw_bus_date and active resp. The .csv file supports multiple database and table with different PK's. The script
#                       only identifies the partitions in scope and applies the deletes only on those partitions. It then applies the deletion to the filesand writes only into those partitions ny overwriting
# THIS IS BASED ON MOVING REMOVING FILES AROUND

import pandas as pd
from pyspark.sql import SQLContext, SparkSession
#from pyspark.sql.functions import col, expr, when, lit, concat, date_add, current_date, date_format,broadcast
from pyspark.sql.functions import broadcast
import subprocess
import time


start_time = time.time()

# Set the spark session and conf parameters
spark = SparkSession.builder.appName("Data Deletion Spark Main App").getOrCreate()
spark.conf.set("spark.debug.maxToStringFields", 100)
spark.sparkContext.setLogLevel("ERROR")
filename = spark.conf.get("spark.key1")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1*1024*1024*1024)
#spark.conf.set("spark.sql.parquet.writeLegacyFormat",'true')



# Step 1: Read the data correction sheet and put into a spark dataframe
pandas_df = pd.read_csv(filename)
print("Total No of data deletions :", pandas_df.shape[0])

# Step 2: Retrieve and store the database,table and PK names
grouped = pandas_df.groupby(['DATABASE_NAME','TABLE_NAME', 'PK','PARTITION_COL'])
for name,group in grouped:
    del_df = spark.createDataFrame(group)
    #del_df.show()
    (g_DATABASE_NAME,g_TABLE_NAME, g_PK,g_PARTITION_COL) = name
    print("\nStarting data deletion for :",g_DATABASE_NAME, g_TABLE_NAME, " for ",group.shape[0],"primary key(s)")

    second_time = time.time()

    # Step 2: Get the table into the dataframe.
    m_sql = 'select * from '+g_DATABASE_NAME+'.'+g_TABLE_NAME
    s_sql = 'describe formatted '+g_DATABASE_NAME+'.'+g_TABLE_NAME
    df = spark.sql(m_sql)
    #df.show()
    print("\tTotal Records before deletion: ", df.count())

    # Step 3: Get the hive table file location
    schema = spark.sql(s_sql).filter('col_name = "Location:"').select('data_type')
    location = schema.collect()
    hive_table_location = location[0].data_type


    # Step 4: Create a sorted list of columns for the union to work properly. Take any dataframe to get the schema
    col_list = df.schema.names
    s_list = [i for i in col_list]

    # Step 5: Join the 2 dataframes using a left outer condition

    # Step 5a: Join the full table with the csv dataframe
    cond = [df[g_PK] == del_df['PK_VALUE']]
    no_recs_to_delete_df = df.join(broadcast(del_df), cond, "inner")
    print("\tNo of records found for deletion: ", no_recs_to_delete_df.count())

    # Step 5b: Get a list of partitions that are impacted
    #partitions_in_scope = df.join(broadcast(del_df), cond, "inner").selectExpr(g_PARTITION_COL+" as _p").distinct()
    partitions_in_scope = no_recs_to_delete_df.selectExpr(g_PARTITION_COL+" as _p").distinct()
    #partitions_in_scope.show()
    partitions = partitions_in_scope.collect()
    print("\tPartitions impacted are :",partitions)


    # Step 5c: Join the main dataframe with the  partitions_in_scope dataframe. This will give only the records that are in the partitions
    all_partitions_df = df.join(partitions_in_scope,df[g_PARTITION_COL]== partitions_in_scope['_p'],'inner').select(s_list)
    print("\tNo of records in partitions before deletion:", all_partitions_df.count())

    this_final_df = all_partitions_df.join(broadcast(del_df),all_partitions_df[g_PK]==del_df['PK_VALUE'], "leftanti").select(s_list)
    print("\tNo of records in partitions after deletion:", this_final_df.count())

    third_time= time.time()

#    if this_final_df.count() > 0:
    final_partitions = this_final_df.selectExpr(g_PARTITION_COL+" as _p").distinct()
    f_partitions = final_partitions.collect()

    temp_directory = hive_table_location + '/_temp'
    #print("\tWriting to :",temp_directory)
    this_final_df.write.format('parquet').mode("overwrite").partitionBy(g_PARTITION_COL).save(temp_directory)

    for j in partitions:
        partition_directory = hive_table_location + '/' + g_PARTITION_COL + '=' + j._p
        #print("\tRemoving files from :", partition_directory)
        subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', partition_directory], stdout=subprocess.DEVNULL)

    for i in f_partitions:
        #partition_directory = hive_table_location + '/' + g_PARTITION_COL + '=' + i._p+'/*'
        original_dir = hive_table_location + '/' + g_PARTITION_COL + '=' + i._p

        mov_dir = temp_directory+'/'+ g_PARTITION_COL + '=' + i._p
        #print("\tMoving files from :", mov_dir, "...to..", original_dir)
        subprocess.call(['hdfs', 'dfs', '-mv', mov_dir,original_dir],stdout=subprocess.DEVNULL)
        print("\tWriting of partitions completed for partition :", g_PARTITION_COL, '=', i._p, "in ",round(time.time()- third_time, 2),"seconds in ", original_dir)
        #print("Removing temp:", temp_directory)

    subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', temp_directory], stdout=subprocess.DEVNULL)

    #df_post_del = spark.sql(m_sql)
    #df_post_del = spark.read.parquet(hive_table_location)
    refresh_stmt = "REFRESH TABLE "+g_DATABASE_NAME + '.' + g_TABLE_NAME
    spark.sql(refresh_stmt)
    df_post_del = spark.sql(m_sql)
    print("\tTotal Records after deletion :",df_post_del.count())

    # Step 5d: Create a temporary view
    #this_final_df.createOrReplaceTempView('TempTable')


    # Step 6: Start writing to the partitions
    #s_list.remove(g_PARTITION_COL)
    #str1 = ','.join(s_list)
    #print(str1)


    #for i in partitions:
     #   third_time = time.time()
       # partition_directory = hive_table_location+'/'+g_PARTITION_COL+'='+i._p
      #  insert_stmt = 'INSERT OVERWRITE TABLE ' + g_DATABASE_NAME + '.' + g_TABLE_NAME + ' partition ('+g_PARTITION_COL+'="'+i._p+'") SELECT '+str1+' FROM TempTable where '+g_PARTITION_COL+'="'+str(i._p)+'"'
        #print(insert_stmt)

        #spark.sql(insert_stmt)
        #print("\tWriting of partitions completed...for ", g_PARTITION_COL,'=',i._p, "..in..", time.time() - third_time," seconds..in..", partition_directory)

    #spark.catalog.dropTempView('TempTable')
    #df_post_del = spark.sql(m_sql)
    #print("\tNo of records after deletion :",df_post_del.count())

# Step 7: Stop spark & cleanup
print("\nOverall time taken..",time.time()- start_time," seconds\n")
spark.stop()

