# File Name         :   dataDeletionCommonFunctions_v2.py
# Version           :   v3
# Created Date      :   27th August 2019
# Updated date      :   12th January 2020
# Author            :   Satyaki.Basu@barclays.com
# Description       :   This script contains all the functions required to delete or purge data from Hadoop Hive tables
#                       This is a spark script in python. This script will perform the data deletions to the input provided in 'csv'. This script is per table
#                       and takes in only the primary keys of the table in question. This script performs delete by only looking at the impacted partitions and
#                       writing it to the existing hive table location. In addition, the script creates a backup file for teh records that are deleted. Also there is a
#                       _AUDIT folder that contains a .csv file with the following information:  spark_application_id, input_file_name, total_records_before_deletion,
#                           total_records_deleted,deleted_records_directory_location,created_datetime, created_by, deletion_keys_values. The script also deletes from non-partitioned tables

# Design Features   :
#                       1. Partition Based Approach for deletion – this is one of the key design approach to purge data based on the partitions rather than a full refresh. This strategy is effective for huge vols of data > 10m and tables having large no of partitions.
#                                       This approach reduces the I/O disk write & shuffling of data over the network. (A full refresh is faster as this overwrites the data but memory management is important). Incase teh tables are non-partitioned, it will calculate and delete from the hive location
#                       2. Table Types – This works on Type 2, Transactional based tables and can be re-used for any other database models.
#                       3. Single & Composite Primary Keys (PK) – This can delete data based on a Single or Composite Primary keys*
#                       4. Single Code base – the script is designed to take database, table names and PK as input. Hence a single code base is effective for maintainability to support multiple use cases.
#                       5. Spark Processing framework – the entire design has been factored using Spark 2.1.1 framework. This is in-memory and distributed processing which make the overall processing faster. The code has been optimized for memory management **
#                       6. Agnostic of Storage format – The script is agnostic of the a) Storage format - parquet, avro or ORC based, b) Compression techniques - snappy, gzip etc or even c) Partition strategy. This works out of the Hive External tables hence does not have any impact to changes to the storage format.
#                       7. Auto detection of meta-data – The script detects the partition strategy & hive file location automatically without the need for special input. It uses this information to write back the data into the same location seamlessly.
#                       8. External calls – Code is wrapped in a shell script which can be callable by any ETL tools (A>I, Informatica etc) or Scheduler (Tivoli)
#                       9. Write optimization – The script ensures that the data written is fully optimized and as per Hadoop recommendation. Hence it uses snappy compression (40% compression) & parquet and compacts the data automatically as per the HDFS block size storage. So 100 files will be rewritten to 10 files effectively – compaction process.
#                       10. Audit and Backup - the script produces and audit of the job run and also creates a backup of teh records that are deleted. This will be used for restoration process in case of accidental deletes.



import time
import subprocess
import re
import datetime
import getpass
import pandas as pd
import pyspark
from pyspark.sql.functions import broadcast


#Function that deletes from Hive table based on set of input records. This takes care of partitioned and non-partitioned type of tables
def deleteFromHiveTable(spark,database_name, table_name,csv_file,applicationId, working_dir=None):
    database_name = database_name
    table_name = table_name
    file_name = csv_file
    m_applicationId = applicationId
    process_loc = working_dir


    # Step 1: Get the hive table file location & partition

    #s_sql = 'describe formatted '+database_name+'.'+table_name
    s_sql = ''.join(['describe formatted ',database_name,'.',table_name])
    version = spark.version
    v = re.search('2.3',version)
    if(v):
        schema = spark.sql(s_sql).filter('col_name = "Location"').select('data_type').coalesce(1)
    else:
        schema = spark.sql(s_sql).filter('col_name = "Location:"').select('data_type').coalesce(1)

    try:
        schema_2 = spark.sql('show partitions ' +database_name+'.'+table_name).distinct().select('partition').first()
        d_partition_col = schema_2.partition.split('=')[0]
    except Exception as e:
        print("INFO: No partitions found..")
        d_partition_col = ""


    location = schema.collect()
    hive_table_location = location[0].data_type


    # Step 2: Read the data correction sheet and put into a spark dataframe
    try:
        pandas_df = pd.read_csv(file_name)
        print("Total no of keys to delete: ", pandas_df.shape[0], "from ", database_name, table_name)
    except Exception as e:
        print("ERROR: Empty file...exiting program")
        exit(1)

    # Step 2a: Build up the query string
    primary_key_list = pandas_df.columns.values.tolist()

    d = pandas_df.to_dict('list')
    y=[]
    for i in d:
        v = "'" + "','".join(str(j) for j in d.get(i)) + "'"
        u = str(i) + ' IN (' + v + ' )'
        y.append(u)
    sub_query = ' AND '.join(str(z) for z in y)


    # Step 3: Get the table into the dataframe.
    second_time = time.time()
    #f_sql = 'select count(*) as rec_count from '+database_name+'.'+table_name
    f_sql = ''.join(['select count(*) as rec_count from ',database_name,'.',table_name])

    if d_partition_col == 'active':
        #m_sql = 'select * from '+database_name+'.'+table_name+" where "+ sub_query + " and active = 0"
        m_sql = ''.join(['select * from ',database_name,'.',table_name," where ",sub_query, " and active = 0"])
    else:
        #m_sql = 'select * from ' + database_name + '.' + table_name + " where " + sub_query
        m_sql = ''.join(['select * from ', database_name, '.', table_name, " where ", sub_query])

    no_recs_to_delete_df = spark.sql(m_sql)


    #Step 3a: Get the list of keys for actual deletion
    keys_values = no_recs_to_delete_df.select(primary_key_list).distinct()
    print("\tTotal no of keys found to delete: ", keys_values.count())
    if keys_values.count() == 0:
        print("INFO: Nothing to delete....exiting program")
        exit(1)


    k1_values = keys_values.toPandas().values.tolist()
    k_values = ""
    for a in k1_values:#[[1234,454],[4567,4544]]
        c = "'" + "~".join(str(b) for b in a)+"'"#[1234,454]
        if k_values:
            k_values = k_values + "|" + c
        else:
            k_values = c



    df = spark.sql(f_sql).collect()
    df_count = [i.rec_count for i in df]
    print("\tTotal Records before deletion: ", df_count[0])
    no_recs_to_delete_df_count = no_recs_to_delete_df.count()
    print("\tTotal Records for deletion: ", no_recs_to_delete_df_count)

    # Step 3b: Take the backup of the records that needs to be deleted prior to processing

    if process_loc:
        backup_directory = process_loc+'/'+database_name+'/'+table_name+'/_BACKUP/backup_'+m_applicationId
    else:
        backup_directory = hive_table_location + '/_BACKUP/backup_' + m_applicationId

    if no_recs_to_delete_df_count > 0 :
        try:
            no_recs_to_delete_df.write.format('parquet').mode("overwrite").partitionBy(d_partition_col).save(backup_directory)
        except:
            no_recs_to_delete_df.write.format('parquet').mode("overwrite").save(backup_directory)
        finally:
            print("\tBackup of records to be deleted written to: ", backup_directory)
            print("\tNo of records backed-up :", no_recs_to_delete_df_count)


    # Step 4: Get a list of partitions that are impacted
    if d_partition_col:
        # Step 4a: Join the main dataframe with the partitions_in_scope dataframe. This will give only the records that are in the partitions
        partitions_in_scope = no_recs_to_delete_df.selectExpr(d_partition_col+" as p").distinct()
        partitions = partitions_in_scope.collect()
        print("\tPartitions [",d_partition_col, "] impacted are (",len(partitions),"): ",partitions)
        str_partition = "'" + "','".join(i.p for i in partitions) + "'"
        p_sql = 'select * from '+database_name+'.'+table_name+" where "+d_partition_col+" IN ("+str_partition+" )"
        all_partitions_df = spark.sql(p_sql)
    else:
        p_sql = 'select * from ' + database_name + '.' + table_name
        all_partitions_df = spark.sql(p_sql) # this will be on the full dataset

    # Set the storage to use memory and disk as inactive partition or no partitions may require huge memory
    all_partitions_df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

    all_partitions_df_count = all_partitions_df.count()
    print("\tNo of records in partitions before deletion:", all_partitions_df_count)

    # Step 5b: Create a list of columns
    col_list = all_partitions_df.schema.names
    s_list = [i for i in col_list]

    # Step 5c: Join the two dataframes and get the result set
    this_final_df = all_partitions_df.join(broadcast(no_recs_to_delete_df),primary_key_list, "leftanti").select(s_list)
    this_final_df_count = this_final_df.count()
    print("\tNo of records in partitions after deletion:", this_final_df_count)


    if this_final_df_count > 0 and d_partition_col:
    # Step 5d: Get the final set of impacted partitions
        final_partitions = this_final_df.selectExpr(d_partition_col + " as p").distinct()
        f_partitions = final_partitions.collect()
        print("\tFinal partitions [",d_partition_col, "] are (",len(f_partitions),"): ", f_partitions)

    # Step 6: Start writing into the partitions
    third_time= time.time()

    if process_loc:
        temp_directory = process_loc+'/'+database_name+'/'+table_name+'/temp_'+m_applicationId
    else:
        temp_directory = hive_table_location + '/temp_' + m_applicationId

    if this_final_df_count > 0:
        try:
            this_final_df.write.format('parquet').mode("overwrite").partitionBy(d_partition_col).save(temp_directory)
        except:
            this_final_df.write.format('parquet').mode("overwrite").save(temp_directory)

    # Step 6b: Removing data from the current partitions
    try:
        if d_partition_col:
            for j in partitions:
                partition_directory = hive_table_location + '/' + d_partition_col + '=' + j.p
                subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', partition_directory], stdout=subprocess.DEVNULL)
        else:
            files = getHDFSFiles(hive_table_location)

            for file_path in files:
                subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', file_path],stdout=subprocess.DEVNULL)
            '''
            partition_directory = hive_table_location + '/*.parquet'
            subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', partition_directory], stdout=subprocess.DEVNULL)
            partition_directory = hive_table_location + '/*_0'
            subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', partition_directory], stdout=subprocess.DEVNULL)
            '''
    except Exception as e:
        print("ERROR: Could not remove data from main location....exiting program")
        print(e.args)
        exit(1)

    # Step 6c: Read from temp and write to main hive location
    try:
        if this_final_df_count > 0:
            if d_partition_col:
                df_temp = spark.read.parquet(temp_directory)
                df_temp.write.format('parquet').mode("append").partitionBy(d_partition_col).save(hive_table_location)
                print("\tWriting of partitions completed.....in...", round(time.time() - third_time,2), "seconds.....in.....",hive_table_location)
            else:
                df_temp = spark.read.parquet(temp_directory)
                df_temp.write.format('parquet').mode("append").save(hive_table_location)
                print("\tWriting of partitions completed.....in...", round(time.time() - third_time, 2), "seconds.....in.....", hive_table_location)
    except Exception as e:
        #restoreDataFromBackup(spark, 'backup', database_name, table_name, m_applicationId,backup_directory)
        #restoreDataFromBackup(spark, 'temp', database_name, table_name, m_applicationId,temp_directory)
        print("ERROR: Could not retrieve data from temp dir:",temp_directory,". Manually restore data from temp dir and backup dir:",backup_directory,"...exiting program")
        exit(1)
    else:
        # Step 6d: Removing data from the temp directory
        if this_final_df_count > 0:
            subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', temp_directory], stdout=subprocess.DEVNULL)
    finally:
        # Create the audit file
        if no_recs_to_delete_df_count > 0:
            createAuditFile(spark, hive_table_location, m_applicationId, file_name, df_count[0], no_recs_to_delete_df_count, backup_directory,k_values, d_partition_col)
        else:
            createAuditFile(spark, hive_table_location, m_applicationId, file_name, df_count[0], 'None', backup_directory,k_values, d_partition_col)
        all_partitions_df.unpersist()
    #remove his code
    #restoreDataFromBackup(spark, 'backup', database_name, table_name,m_applicationId,backup_directory)

#Function to restore data from any backup directory
def restoreDataFromBackup(spark,folder,database_name, table_name, applicationId,restore_dir):
    m_applicationId = applicationId
    print("Restoring data for ", database_name, table_name,restore_dir)
    start_time = time.time()

    # Step 1: Get the Hive location and partition key
    s_sql = 'describe formatted '+database_name+'.'+table_name
    version = spark.version
    v = re.search('2.3', version)
    if(v):
        schema = spark.sql(s_sql).filter('col_name = "Location"').select('data_type').coalesce(1)
    else:
        schema = spark.sql(s_sql).filter('col_name = "Location:"').select('data_type').coalesce(1)

    try:
        schema_2 = spark.sql('show partitions ' +database_name+'.'+table_name).distinct().select('partition').first()
        d_partition_col = schema_2.partition.split('=')[0]
    except:
        print("No partitions found..")
        d_partition_col = ""

    location = schema.collect()
    hive_table_location = location[0].data_type

    f_sql = 'select count(*) as rec_count from '+database_name+'.'+table_name

    df = spark.sql(f_sql).collect()
    df_count = [i.rec_count for i in df]
    no_recs_before_rest = df_count[0]
    print("\tTotal Records before restoration: ", no_recs_before_rest)

    # Step 2: Restore data from backup
    #backup_directory = hive_table_location+'/'+folder+'_'+application_id
    backup_directory = restore_dir

    df_backup = spark.read.parquet(backup_directory)
    print("\tRestoring data from backup directory: ", backup_directory)
    no_of_recs_restore = df_backup.count()
    print("\tNo of records to restore :", no_of_recs_restore)
    try:
        if d_partition_col:
            df_backup.write.format('parquet').mode("append").partitionBy(d_partition_col).save(hive_table_location)
            print("\tData restoration complete in: ", time.time() - start_time," seconds")
        else:
            df_backup.write.format('parquet').mode("append").save(hive_table_location)
            print("\tData restoration complete in: ", time.time() - start_time, " seconds")
    except Exception as e:
        print(e.args)
        exit(1)
    else:
        # Step 2b: Removing data from the backup directory
        subprocess.call(['hdfs', 'dfs', '-rm', '-r', '-skipTrash', backup_directory], stdout=subprocess.DEVNULL)
        print("\tBackup directory removed: ", restore_dir)

    # Step 3: Do a full refresh of the cache and get the final results
    refresh_stmt = "REFRESH TABLE " + database_name + '.' + table_name
    spark.sql(refresh_stmt)
    df = spark.sql(f_sql).collect()
    df_count = [i.rec_count for i in df]
    print("\tTotal Records after restoration: ", df_count[0])

#Function to create audit file
def createAuditFile(spark,hive_table_location,application_id,input_file_name,total_records_before_deletion,total_records_deleted, deleted_records_directory_location,keys_and_values, partition_value):
    create_timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    user = getpass.getuser()
    if partition_value:
        audit_location = hive_table_location + '/AUDIT'
    else:
        audit_location = hive_table_location + '/_AUDIT'
    audit_list = []

    audit_list.append((application_id, input_file_name, total_records_before_deletion, total_records_deleted, deleted_records_directory_location,create_timestamp, user,keys_and_values))

    # Write the audit list to a file
    audit_pd = pd.DataFrame(audit_list, columns=['spark_application_id', 'input_file_name', 'total_records_before_deletion',
                                                 'total_records_deleted', 'deleted_records_directory_location',
                                                 'created_datetime', 'created_by','deleted_keys_and_values'])
    audit_df = spark.createDataFrame(audit_pd)
    audit_df.coalesce(1).write.format('csv').mode('append').option("header", "true").save(audit_location)

    # Rename the audit file
    subprocess.call(['hdfs', 'dfs', '-mv', audit_location + '/part*.csv', audit_location + '/' + 'audit_' + application_id + '.csv'])
    print('\tAudit file created in :', audit_location)

#This provides a list of files in a particular directory. It does not include any directories
def getHDFSFiles(path):
    args_tdir = "hdfs dfs -ls " + path + " | awk '{print $1}'"
    args_dir = "hdfs dfs -ls " + path + " | awk '{print $8}'"

    proc_tdir = subprocess.Popen(args_tdir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    proc_dir = subprocess.Popen(args_dir, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

    s_tdir, s_err = proc_tdir.communicate()
    s_dir, s_err1 = proc_dir.communicate()

    all_tdir = s_tdir.split()[1:]  # stores type of files and sub-directories in 'path'
    all_dir = s_dir.split()  # stores list of files and sub-directories in 'path'

    a = dict(zip(all_dir, all_tdir))

    return [i for i in a if not re.search(r'd', str(a[i]))]

# MAIN FUNCTION #
# File Name         :   dataDeletionFromBIW2_v4.py
# Version           :   v6
# Created Date      :   27th August 2019
# Updated date      :   12th January 2020
# Author            :   Satyaki.Basu@barclays.com
# Description       :   This is the main spark script in python. This script will perform the data deletions to the input provided in 'csv'. This script is per table
#                       and takes in only the primary keys of the table in question. This script performs delete by only looking at the impacted partitions and
#                       writing it to the existing hive table location. In absence of partitions, it will delete teh data from full set. In addition, the script creates a backup file for teh records that are deleted. Also there is a
#                       _AUDIT folder that contains a .csv file with the following information:  spark_application_id, input_file_name, total_records_before_deletion,total_records_deleted,deleted_records_directory_location,created_datetime, created_by, deletion_keys_values.

from pyspark.sql import SparkSession
import time
from dataDeletionCommonFunctions_v2 import deleteFromHiveTable, restoreDataFromBackup, createAuditFile


start_time = time.time()
# Set the spark session and conf parameters
spark = SparkSession.builder.appName("Data Deletion Spark Main App Solo").getOrCreate()
spark.conf.set("spark.debug.maxToStringFields", 100)
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1*1024*1024*1024) # Default is 10485760 ie 10MB
m_applicationId = spark.sparkContext.applicationId

# get the params
database_name = spark.conf.get("spark.key1")
table_name = spark.conf.get("spark.key2")
file_name = spark.conf.get("spark.key3")
process_loc = spark.conf.get("spark.key4")


# call the main function
deleteFromHiveTable(spark,database_name,table_name,file_name,m_applicationId,process_loc)


# Do a full refresh of the cache and get the final results
refresh_stmt = "REFRESH TABLE " + database_name + '.' + table_name
r_sql = 'select count(*) as rec_count from '+database_name+'.'+table_name
spark.sql(refresh_stmt)
df = spark.sql(r_sql).collect()
df_count = [i.rec_count for i in df]
print("\tTotal Records after deletion: ", df_count[0])

# Stop spark & cleanup
print("\nOverall time taken..",time.time()- start_time," seconds\n")
spark.stop()

