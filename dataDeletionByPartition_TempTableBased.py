'''
PK,PK_VALUE,DATABASE_NAME,TABLE_NAME,PARTITION_COL
STG_ID,300010000011393,e_ukba_db,tehcent_satyaki,dw_bus_dt
STG_ID,300010001097885,e_ukba_db,tehcent_satyaki,dw_bus_dt
CUS_IDR,1000000004,E_CUSTOMER_DB,TDLPCUS_satyaki,active
'''


# Command Dev :  spark2-submit --master local[*] --conf spark.key1=dataDeletion_v4.csv --files dataDeletion_v4.csv dataDeletionByPartition_v2.py
# Command Dev :  spark2-submit --master yarn --deploy-mode client --conf spark.key1=dataDeletion_v4.csv --files dataDeletion_v4.csv dataDeletionByPartition_v2.py file://dataDeletion_v4.csv
# Command Prod : spark2-submit --master yarn --deploy-mode cluster --conf spark.key1=dataDeletion_txn_v2.csv --files dataDeletion_txn_v2.csv dataDeletion_txn_v2.py file://dataDeletion_v4.csv

# File Name         :   dataDeletionByPartition_v2.py
# Created Date      :   17th June 2019
# Author            :   Satyaki.Basu@barclays.com
# Description       :   This is a spark script in python. This script will perform the data deletions to the input provided in dataDeletion_v4.csv. This script will work for
#                       transactional & type 2 tables partitioned by dw_bus_date and active resp. The .csv file supports multiple database and table with different PK's. The script
#                       only identifies the partitions in scope and applies the deletes only on those partitions. It then applies the deletion to the filesand writes only into those partitions ny overwriting
# THIS IS BASED ON INSERT TABLE APPROACH

import pandas as pd
from pyspark.sql import SQLContext, SparkSession
#from pyspark.sql.functions import col, expr, when, lit, concat, date_add, current_date, date_format,broadcast
from pyspark.sql.functions import broadcast

import time


start_time = time.time()

# Set the spark session and conf parameters
spark = SparkSession.builder.appName("Data Deletion Spark Main App").getOrCreate()
spark.conf.set("spark.debug.maxToStringFields", 100)
spark.sparkContext.setLogLevel("ERROR")
filename = spark.conf.get("spark.key1")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1*1024*1024*1024)



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
    print("\tPartitions impacted are....",partitions)


    # Step 5c: Join the main dataframe with the  partitions_in_scope dataframe. This will give only the records that are in the partitions
    all_partitions_df = df.join(partitions_in_scope,df[g_PARTITION_COL]== partitions_in_scope['_p'],'inner').select(s_list)
    print("\tNo of records in partitions before deletion:", all_partitions_df.count())

    this_final_df = all_partitions_df.join(broadcast(del_df),all_partitions_df[g_PK]==del_df['PK_VALUE'], "leftanti").select(s_list)
    print("\tNo of records in partitions after deletion:", this_final_df.count())

    # Step 5d: Create a temporary view
    this_final_df.createOrReplaceTempView('TempTable')


    # Step 6: Start writing to the partitions
    s_list.remove(g_PARTITION_COL)
    str1 = ','.join(s_list)
    #print(str1)


    for i in partitions:
        third_time = time.time()
        partition_directory = hive_table_location+'/'+g_PARTITION_COL+'='+i._p
        insert_stmt = 'INSERT OVERWRITE TABLE ' + g_DATABASE_NAME + '.' + g_TABLE_NAME + ' partition ('+g_PARTITION_COL+'="'+i._p+'") SELECT '+str1+' FROM TempTable where '+g_PARTITION_COL+'="'+str(i._p)+'"'
        #print(insert_stmt)

        spark.sql(insert_stmt)
        print("\tWriting of partitions completed...for ", g_PARTITION_COL, '=', i._p, "....in...", time.time() - third_time," seconds.....in.....", partition_directory)

    spark.catalog.dropTempView('TempTable')
    df_post_del = spark.sql(m_sql)
    print("\tNo of records after deletion :",df_post_del.count())

'''
    # Step 6a: Run a drop partition for the partitions in scope
    for i in partitions:
        partition_directory = hive_table_location
        this_final_df.write.format('parquet').mode("append").partitionBy(g_PARTITION_COL).save(partition_directory+'/')
        print("\tWriting of partitions completed...for ",g_PARTITION_COL,'=',i._p,"....in...", time.time() - third_time, " seconds.....in.....",partition_directory, "\n")

        # Step 6d: Alter the table to add partitions
        add_partition_stmt = 'ALTER TABLE ' + g_DATABASE_NAME + '.' + g_TABLE_NAME + ' ADD PARTITION (' + g_PARTITION_COL + '= "' + i._p + '")'
        print(add_partition_stmt)
        #spark.sql(add_partition_stmt)
'''
# Step 7: Stop spark & cleanup
print("\nOverall time taken..",time.time()- start_time," seconds\n")
spark.stop()



# Command Dev : spark2-submit --master local[*] --conf spark.key1=dataDeletion_txn_v2.csv --files dataDeletion_txn_v2.csv dataDeletion_txn_v2.py
# Command Dev : spark2-submit --master yarn --deploy-mode client --conf spark.key1=dataDeletion_txn_v2.csv --files dataDeletion_txn_v2.csv dataDeletion_txn_v2.py file://dataDeletion_txn_v2.csv
# Command Prod : spark2-submit --master yarn --deploy-mode cluster --conf spark.key1=dataDeletion_txn_v2.csv --files dataDeletion_txn_v2.csv dataDeletion_txn_v2.py file://dataDeletion_txn_v2.csv

# File Name         :   dataDeletion_txn_v2.py
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
import subprocess

start_time = time.time()

# Set the spark session and conf parameters
spark = SparkSession.builder.appName("Data Deletion Spark Main App").getOrCreate()
spark.conf.set("spark.debug.maxToStringFields", 100)
spark.sparkContext.setLogLevel("ERROR")
filename = spark.conf.get("spark.key1")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1*1024*1024*1024)



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
    cond = [df[g_PK] == del_df['PK_VALUE']]
    no_recs_to_delete = df.join(broadcast(del_df), cond, "inner").count()
    print("\tNo of records found for deletion: ", no_recs_to_delete)

    partitions_in_scope = df.join(broadcast(del_df), cond, "inner").selectExpr(g_PARTITION_COL+" as _p").distinct()
    #print("\tPartitions impacted are....")
    #partitions_in_scope.show()
    partitions = partitions_in_scope.collect()
    print("\tPartitions impacted are....",partitions)

    #    for i in partitions:
 #       print(i._p)
    #print(partitions)
    #no_recs_to_delete.show()

    final_df = df.join(broadcast(del_df), cond, "leftanti").select(s_list)
    #print("FULL Dataset after deletion.....:",final_df.count())

    my_final_df = final_df.join((broadcast(partitions_in_scope)),final_df[g_PARTITION_COL] == partitions_in_scope['_p'],'inner').select(s_list)
    #my_final_df.show()
    count_my_final = my_final_df.count()
    print("\tNo of records to re-build in impacted partitions :",count_my_final)

    # Run a drop partition for the partitions in scope
    for i in partitions:
        #alter_partition_stmt = 'ALTER TABLE '+g_DATABASE_NAME+'.'+g_TABLE_NAME+ ' DROP PARTITION ('+g_PARTITION_COL+'= "'+i._p+'" )'
        #print("\tDropping partitions....",g_PARTITION_COL,'=',i._p)
        #spark.sql(alter_partition_stmt)

        third_time = time.time()
        partition_directory = hive_table_location + '/GRMS/'
        #my_final_df.write.format('parquet').mode("append").partitionBy(g_PARTITION_COL).save(partition_directory+'/'+g_PARTITION_COL+'='+i._p)
        my_final_df.write.format('parquet').mode("append").partitionBy(g_PARTITION_COL).save(partition_directory+'/')

        #print("Written final partitions....",count_my_final)
        print("\tWriting of partitions completed...for ",g_PARTITION_COL,'=',i._p,"....in...", time.time() - third_time, " seconds.....in.....",partition_directory, "\n")
    '''

    #print("\tNo of records found for deletion: ", no_recs_to_delete)

    final_df = df.join(broadcast(del_df), cond, "leftanti").select(s_list)
    print("\tTotal Records after deletion: ",final_df.count())

    # Step 6: Write this to a new location
    third_time = time.time()
    partition_directory = hive_table_location+'/GRMS/'
    final_df.write.format('parquet').mode("overwrite").partitionBy(g_PARTITION_COL).save(partition_directory)
    print("\tWriting of partitions completed.....in...", time.time() - third_time, " seconds.....in.....",partition_directory,"\n")
'''
# Step 7: Stop spark
print("Overall time taken..",time.time()- start_time," seconds\n")
spark.stop()





