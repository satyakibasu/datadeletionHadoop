# Command Dev :  spark2-submit --master local[*] --conf spark.key1=E_UKBA_DB --conf spark.key2=TEHCENT_SATYAKI --conf spark.key3=STG_ID --conf spark.key4=data/dataDeletionPartitionSolo.csv --files data/dataDeletionPartitionSolo.csv dataDeletionByPartitionSolo_v2.py
# Command Dev/SIT : spark2-submit --master yarn --deploy-mode client --num-executors 50 --executor-cores 5 --executor-memory 30g --driver-memory 2g --conf spark.kryoserializer.buffer.max=512m --conf spark.key1=$1 --conf spark.key2=$2 --conf spark.key3=$3 --conf spark.key4=data/$4 --files data/$4 dataDeletionByPartitionSolo_v2.py
# Command Prod : spark2-submit --master yarn --deploy-mode cluster --executor-cores 5 --conf spark.key1=$1 --conf spark.key2=$2 --conf spark.key3=$3 --conf spark.key4=$4 --files data/$4 dataDeletionByPartitionSolo_v2.py

# Calling script : sh dataDeletionByPartitionSolo_cluster.sh E_UKBA_DB TEHCENT_SATYAKI STG_ID tehcent.csv

# File Name         :   dataDeletionByPartitionSolo_v2.py
# Created Date      :   14th July 2019
# Author            :   Satyaki.Basu@barclays.com
# Description       :   This is a spark script in python. This script will perform the data deletions to the input provided in 'csv'. This script is per table
#                       and takes in only the primary keys of the table in question. This script performs delete by only looking at the impacted partitions and
#                       writing it to the existing hive table location.


import pandas as pd
from pyspark.sql import SQLContext, SparkSession
import subprocess
import time


start_time = time.time()

# Set the spark session and conf parameters
spark = SparkSession.builder.appName("Data Deletion Spark Main App Solo").getOrCreate()
spark.conf.set("spark.debug.maxToStringFields", 100)
spark.sparkContext.setLogLevel("ERROR")
#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1*1024*1024*1024)
#spark.conf.set("spark.sql.parquet.writeLegacyFormat",'true')

config = spark.sparkContext.getConf().getAll()
#print(config)

database_name = spark.conf.get("spark.key1")
table_name = spark.conf.get("spark.key2")
primary_key = spark.conf.get("spark.key3")
file_name = spark.conf.get("spark.key4")


# Step 1: Get the hive table file location & partition
s_sql = 'describe formatted '+database_name+'.'+table_name
schema = spark.sql(s_sql).filter('col_name = "Location:"').select('data_type')
schema_2 = spark.sql('show partitions ' + database_name + '.' + table_name).distinct().select('partition').first()

location = schema.collect()
hive_table_location = location[0].data_type
d_partition_col = schema_2.partition.split('=')[0]


# Step 2: Read the data correction sheet and put into a spark dataframe
pandas_df = pd.read_csv(file_name)
print("Total no of keys to delete: ", pandas_df.shape[0], "from ",database_name,table_name)
data_keys = pandas_df['PK_VALUE'].values.tolist()
#str_data = ','.join(str(y) for y in data_keys)                 #This one assumed that PK are numeric
str_data = "'" + "','".join(str(y) for y in data_keys) + "'"    #This is for alpha-numeric keys


# Step 3: Get the table into the dataframe.
second_time = time.time()
f_sql = 'select count(*) as rec_count from '+database_name+'.'+table_name
m_sql = 'select '+primary_key+', '+d_partition_col+' from '+database_name+'.'+table_name+" where "+primary_key+" IN ("+str_data+" )"
no_recs_to_delete_df = spark.sql(m_sql)
df = spark.sql(f_sql).collect()
df_count = [i.rec_count for i in df]
print("\tTotal Records before deletion: ", df_count[0])
print("\tTotal Records for deletion: ", no_recs_to_delete_df.count())


# Step 4: Get a list of partitions that are impacted
partitions_in_scope = no_recs_to_delete_df.selectExpr(d_partition_col+" as p").distinct()
#partitions_in_scope.show()
partitions = partitions_in_scope.collect()
print("\tPartitions impacted are (",len(partitions),"): ",partitions)
str_partition = "'" + "','".join(i.p for i in partitions) + "'"


# Step 5a: Join the main dataframe with the partitions_in_scope dataframe. This will give only the records that are in the partitions
p_sql = 'select * from '+database_name+'.'+table_name+" where "+d_partition_col+" IN ("+str_partition+" )"
all_partitions_df = spark.sql(p_sql)
print("\tNo of records in partitions before deletion:", all_partitions_df.count())

# Step 5b: Create a list of columns
col_list = all_partitions_df.schema.names
s_list = [i for i in col_list]

# Step 5c: Join teh two dataframes and get the result set
this_final_df = all_partitions_df.join(no_recs_to_delete_df,all_partitions_df[primary_key]==no_recs_to_delete_df[primary_key], "leftanti").select(s_list)
this_final_df_count = this_final_df.count()
print("\tNo of records in partitions after deletion:", this_final_df_count)

# Step 5d: Get the final set of impacted partitions
final_partitions = this_final_df.selectExpr(d_partition_col + " as p").distinct()
f_partitions = final_partitions.collect()
print("\tFinal partitions are (",len(f_partitions),"): ", f_partitions)



# Step 6: Start writing into the partitions
third_time= time.time()
if this_final_df_count > 0:
    temp_directory = hive_table_location + '/temp/'
    #print("\tWriting to :",temp_directory)
    this_final_df.write.format('parquet').mode("overwrite").partitionBy(d_partition_col).save(temp_directory)
    #print("\tWriting of partitions in temp dir completed.....in...", round(time.time() - third_time,2), "seconds.....in.....",temp_directory)

# Step 6b: Removing data from the current partitions
for j in partitions:
    partition_directory = hive_table_location + '/' + d_partition_col + '=' + j.p
    # print("\tRemoving files from :", partition_directory)
    subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', partition_directory], stdout=subprocess.DEVNULL)
    # print("\tRemoving files complete from :", partition_directory)

# Step 6c: Read from temp and write to main hive location
if this_final_df_count > 0:
    df_temp = spark.read.parquet(temp_directory)
    df_temp.write.format('parquet').mode("append").partitionBy(d_partition_col).save(hive_table_location)
    print("\tWriting of partitions completed.....in...", round(time.time() - third_time,2), "seconds.....in.....",hive_table_location)

    # Step 6d: Removing data from the temp directory
    subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', temp_directory], stdout=subprocess.DEVNULL)

# Step 6e: Do a full refresh of the cache and get the final results
refresh_stmt = "REFRESH TABLE " + database_name + '.' + table_name
spark.sql(refresh_stmt)

# Step 7: Stop spark & cleanup
print("\nOverall time taken..",time.time()- start_time," seconds\n")
spark.stop()
