# Databricks notebook source
# MAGIC %md # This notebook does duplicate file check and validates naming standards:

# COMMAND ----------

# DBTITLE 1,Importing pyspark libraries:
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

from datetime import *
import time
import datetime
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import pgpy

# COMMAND ----------

# DBTITLE 1,Create widgets and get values:
dbutils.widgets.removeAll()

dbutils.widgets.text("filename", "")
file_name = dbutils.widgets.get("filename")

dbutils.widgets.text("sourcetm", "")
source_tms = dbutils.widgets.get("sourcetm")

dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")

dbutils.widgets.text("storageaccountname", "")
storage_account_name = dbutils.widgets.get("storageaccountname")

dbutils.widgets.text("feed_name", "")
feed_name = dbutils.widgets.get("feed_name")

dbutils.widgets.text("file_prefix", "")
file_prefix = dbutils.widgets.get("file_prefix")

dbutils.widgets.text("adb_par","")
adb_par_dict = json.loads(dbutils.widgets.get("adb_par"))

dbutils.widgets.text("reload_ind", "")
reload_ind = dbutils.widgets.get("reload_ind")

#print("Filename :" + file_name)
print("Source_tms :" + source_tms)
print("Env :" + env)
print("Storageaccountname :" + storage_account_name)
print("reload_ind: " + reload_ind)

# COMMAND ----------

# DBTITLE 1,Parse the Notebook Parameter File
# MAGIC %run "../param/RVACT_parameters_parser"

# COMMAND ----------

# DBTITLE 1,Check file sequence validation
is_file_sequence = True
struct_max_tms = ''

if file_frequency_check_ind == 'Y':
  
  #Checks if file is not a reload file
  if reload_ind == 'N':
    
    #get the latest source data timestamp in struct
    if spii_ind == 'Y':
      sql_stmt = spark.sql("""SELECT MAX(SOURCE_DATA_TMS) AS max_tms 
                     FROM rvact_{0}_struct_spii.{1}""".format(env, struct_table))
      
    elif pii_ind == 'Y':
      sql_stmt = spark.sql("""SELECT MAX(SOURCE_DATA_TMS) AS max_tms 
                     FROM rvact_{0}_struct_pii.{1}""".format(env, struct_table))
    else:
      sql_stmt = spark.sql("""SELECT MAX(SOURCE_DATA_TMS) AS max_tms 
                     FROM rvact_{0}_struct.{1}""".format(env, struct_table))

    try:
      struct_max_tms = sql_stmt.select('max_tms').collect()
      struct_max_tms = [row.max_tms for row in struct_max_tms]
      
      if struct_max_tms[0] != None:
        struct_max_tms = int(struct_max_tms[0])
        
      elif struct_max_tms[0] == None:
        struct_max_tms = ''
        
    except:
      struct_max_tms = ''
      print('struct_max_tms = ' + str(struct_max_tms)) 
      
    if str(struct_max_tms) != '':
      source_data_tms = str(source_data_tms)
      struct_max_tms = str(struct_max_tms)
      source_data_tms = source_data_tms[:4] + '-'+source_data_tms[4:6] + '-'+source_data_tms[6:8] + ' '+source_data_tms[8:10] + ':'+source_data_tms[10:12] + ':'+source_data_tms[12:14]
      struct_max_tms = struct_max_tms[:4] + '-'+struct_max_tms[4:6] + '-'+struct_max_tms[6:8] + ' '+struct_max_tms[8:10] + ':'+struct_max_tms[10:12] + ':'+struct_max_tms[12:14]

      #convert variable to datetime datatype
      source_data_tms = datetime.datetime.strptime(source_data_tms, '%Y-%m-%d %H:%M:%S')
      struct_max_tms = datetime.datetime.strptime(struct_max_tms, '%Y-%m-%d %H:%M:%S')

      #daily files
      if file_frequency == 'Daily' and file_reoccurence == '1': 
        if ((source_data_tms - struct_max_tms).total_seconds()/3600) > 0 and ((source_data_tms - struct_max_tms).total_seconds()/3600) < 26:
          print(f"This file is in expected order: {str(source_data_tms)} ")

        else:
          is_file_sequence = False
          error_msg = f"This file is out of order: {str(source_data_tms)}. Allowable files must be greater than: {str(struct_max_tms)} and should be at most 26 hours time frame."
          print(error_msg)

      elif file_frequency == 'Daily' and file_reoccurence == '2':
        if ((source_data_tms - struct_max_tms).total_seconds()/3600) > 0 and ((source_data_tms - struct_max_tms).total_seconds()/3600) < 14:
          print(f"This file is in expected order: {str(source_data_tms)} ")

        else:
          is_file_sequence = False
          error_msg = f"This file is out of order: {str(source_data_tms)}. Allowable files must be greater than: {str(struct_max_tms)} and should be at most 14 hours time frame."
          print(error_msg)

      #weekly files
      elif file_frequency == 'Weekly' and file_reoccurence == '1':
        if (source_data_tms - struct_max_tms).days > 0 and (source_data_tms - struct_max_tms).days < 8:
          print(f"This file is in expected order: {str(source_data_tms)} ")

        else:
          is_file_sequence = False
          error_msg = f"This file is out of order: {str(source_data_tms)}. Allowable files must be greater than: {str(struct_max_tms)} and should be at most 8 days interval."
          print(error_msg)

      #monthly file
      elif file_frequency == 'Monthly' and file_reoccurence == '1':
        if relativedelta.relativedelta(source_data_tms, struct_max_tms).months == 1:
          print(f"This file is in expected order: {str(source_data_tms)} ")

        else:
          is_file_sequence = False
          error_msg = f"This file is out of order: {str(source_data_tms)}. Allowable files must be greater than: {str(struct_max_tms)} and should be at most 1 month interval."
          print(error_msg)
    else:
      print("No record in the table")
  else:
    print("This is a reload file and there is no need to check file order.")
else:
  print("There is no need to check file order for this file.")

# COMMAND ----------

# DBTITLE 1,Capture File Name
work_file_name = file_name.rstrip(".gz")

# COMMAND ----------

# DBTITLE 1,Define input file path
#data_file_path="abfss://work@{1}.dfs.core.windows.net/{2}".format(adls_storage_cntner,l_storage_account_name,work_file_name)
#print (data_file_path)if encryption_ind == "Y":

# COMMAND ----------

# DBTITLE 1,Copy key & data files from local storage account to dbfs.
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
import pgpy
#encryption_ind='Y'
if encryption_ind == "N":
  client_secret = dbutils.secrets.get(scope =databricks_scopename, key =keyvault_secret_name)
  tenant_id = service_principle_directory_id
  client_id = service_principle_clientid
  oauth_url = "https://{0}.blob.core.windows.net".format(l_storage_account_name)
  token_credential = ClientSecretCredential(tenant_id,client_id,client_secret)
  blob_service_client = BlobServiceClient(account_url=oauth_url, credential=token_credential)

  #Download the encrypted file from local storage ADLS containers to dbfs
  container_client = blob_service_client.get_container_client("landing")
  blob_client = container_client.get_blob_client(file_name)
  dbfs_encrypt_file = "/dbfs/tmp/{0}".format(file_name)
  with open(dbfs_encrypt_file, "wb") as data:
    download_stream = blob_client.download_blob()
    data.write(download_stream.readall())


if encryption_ind == "Y":
  # Instantiate a BlobServiceClient using a token credential to access the local blob storage.
  client_secret = dbutils.secrets.get(scope =databricks_scopename, key =keyvault_secret_name)
  tenant_id = service_principle_directory_id
  client_id = service_principle_clientid
  oauth_url = "https://{0}.blob.core.windows.net".format(l_storage_account_name)
  token_credential = ClientSecretCredential(tenant_id,client_id,client_secret)
  blob_service_client = BlobServiceClient(account_url=oauth_url, credential=token_credential)
 
  #Download the key file from local storage ADLS containers to dbfs
  container_client = blob_service_client.get_container_client("keys")
  blob_client = container_client.get_blob_client(private_key_file_name) 
  dbfs_key_file = "/dbfs/tmp/{0}".format(private_key_file_name)
  with open(dbfs_key_file, "wb") as data:
    download_stream = blob_client.download_blob()
    data.write(download_stream.readall())
  
  #Download the encrypted file from local storage ADLS containers to dbfs
  container_client = blob_service_client.get_container_client("landing-encrypted")
  blob_client = container_client.get_blob_client(file_name)
  dbfs_encrypt_file = "/dbfs/tmp/{0}".format(file_name)
  with open(dbfs_encrypt_file, "wb") as data:
    download_stream = blob_client.download_blob()
    data.write(download_stream.readall())
  

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/

# COMMAND ----------

# DBTITLE 1,Pass python arguments to Unix shell
import os
os.environ['private_key_file_name'] = private_key_file_name  
os.environ['work_file_name'] = work_file_name
os.environ['file_name'] = file_name
key_passphrase = dbutils.secrets.get(scope =databricks_scopename, key =gpgkey_secret_name)
os.environ['key_passphrase'] = key_passphrase
os.environ['encryption_ind']=encryption_ind

# COMMAND ----------

# DBTITLE 1,Uncompress the input file.
# MAGIC %sh -e gunzip /dbfs/tmp/${file_name}

# COMMAND ----------

# DBTITLE 1,Change current directory to root & Import the GPG Private Key.
# MAGIC %sh -e
# MAGIC if [ "$encryption_ind" == "Y" ]
# MAGIC then 
# MAGIC cd /
# MAGIC gpg --batch --import /dbfs/tmp/$private_key_file_name
# MAGIC 
# MAGIC RC=$?
# MAGIC if [ "$RC" != 0 ]
# MAGIC then 
# MAGIC echo "Decryption Failed" $RC
# MAGIC exit $RC
# MAGIC fi
# MAGIC 
# MAGIC fi

# COMMAND ----------

# MAGIC %sh  gpg --list-keys

# COMMAND ----------

# DBTITLE 1,Decrypt the input file using a private key and passphrase(V6.4).
# %sh  -e
#  if [ "$encryption_ind" == "Y" ]
#  then
#  gpg --batch --no-tty --yes --passphrase $key_passphrase --output /dbfs/tmp/${work_file_name}_out --decrypt /dbfs/tmp/${work_file_name}
#  fi

#  RC=$?
#  if [ "$RC" != 0 ]
#  then 
#  echo "Decryption Failed" $RC
#  exit $RC
#  fi

# COMMAND ----------

# DBTITLE 1,Decrypt the input file using a private key and passphrase.
# MAGIC %sh -e
# MAGIC  if [ "$encryption_ind" == "Y" ]
# MAGIC  then
# MAGIC  gpg --batch --pinentry-mode loopback --no-tty --yes --passphrase $key_passphrase --output /dbfs/tmp/${work_file_name}_out --decrypt /dbfs/tmp/${work_file_name}
# MAGIC  fi
# MAGIC 
# MAGIC  RC=$?
# MAGIC  if [ "$RC" != 0 ]
# MAGIC  then 
# MAGIC  echo "Decryption Failed" $RC
# MAGIC  exit $RC
# MAGIC  fi

# COMMAND ----------

# DBTITLE 1,Decryption using Python:
# if encryption_ind == "Y":
#   #Decrypt using private key & passphrase 
#   dbfs_encrypt_file = "/dbfs/tmp/{0}".format(work_file_name)
#   key_passphrase = dbutils.secrets.get(scope =databricks_scopename, key =gpgkey_secret_name)
#   private_key, _ = pgpy.PGPKey.from_file(str(dbfs_key_file))
#   print (private_key.is_protected)
#   with private_key.unlock(key_passphrase):
#     message_from_file = pgpy.PGPMessage.from_file(dbfs_encrypt_file)
#     raw_message = private_key.decrypt(message_from_file).message
#     #Capture the decrypt data into a file
#   dbfs_decrypt_file = "/dbfs/tmp/{0}_out".format(work_file_name)
#   with open (dbfs_decrypt_file, 'wb') as f:
#     f.write(raw_message)

# COMMAND ----------

# DBTITLE 1,Copy the output files back to local storage ADLS container
if spii_ind == 'Y':
  dest_location="abfss://{0}@{1}.dfs.core.windows.net/{2}/work-spii/{3}".format(adls_storage_cntner_spii,storage_account_name,env,work_file_name)
elif pii_ind == 'Y':
  dest_location="abfss://{0}@{1}.dfs.core.windows.net/{2}/work-pii/{3}".format(adls_storage_cntner_pii,storage_account_name,env,work_file_name)
else:
  dest_location="abfss://{0}@{1}.dfs.core.windows.net/{2}/work/{3}".format(adls_storage_cntner,storage_account_name,env,work_file_name)
  
if encryption_ind == "Y":
  src_location= "dbfs:/tmp/{0}_out".format(work_file_name)
  print(src_location)
  #dest_location="abfss://work@{0}.dfs.core.windows.net/{1}".format(l_storage_account_name,work_file_name)  
  
elif encryption_ind == "N":
  src_location= "dbfs:/tmp/{0}".format(work_file_name)
  print(src_location)
  #dest_location="abfss://work@{0}.dfs.core.windows.net/{1}".format(l_storage_account_name,work_file_name)
print(dest_location)
print(src_location)
dbutils.fs.cp(src_location, dest_location)

# COMMAND ----------

# DBTITLE 1,Define input file path
data_file_path="dbfs:/tmp/{0}".format(work_file_name)
if encryption_ind == "Y":
  data_file_path="dbfs:/tmp/{0}_out".format(work_file_name)

# COMMAND ----------

# DBTITLE 1,Header and Trailer Records Parsing and Count comparision
df_read_input_file = spark.read.text(data_file_path)
file_record_count = df_read_input_file.count()
trailer_count_start_position=int(trailer_rec_count_start)
trailer_count_end_position=int(trailer_rec_count_end)
trailer_count_match_value=int(trailer_count_match_value)

if trailer_ind == "Y": 
  df_read_trailer = spark.read.text(data_file_path).filter(col('value').startswith(trailer_prefix)== True)
  trailer_record = df_read_trailer.withColumn('cutted', expr("substring(value,"+trailer_position+")")).first()
  
  trailer_record_count_new = trailer_record.cutted[trailer_count_start_position:trailer_count_end_position]
  if trailer_record_count_new.__contains__(","):
    trailer_record_count_new = int(trailer_record_count_new.rstrip(","))
  else:
    trailer_record_count_new = int(trailer_record.cutted[trailer_count_start_position:trailer_count_end_position])
    
  record_count=trailer_record_count_new + trailer_count_match_value
  trailer_record =str(trailer_record.cutted)
else:
  trailer_record = spark.sql(""" select 'No_Trailer' cutted """)
  trailer_record=trailer_record.select("cutted").collect()
  trailer_record=trailer_record[0].cutted
  record_count = file_record_count
  
if header_ind == "Y":
  df_read_header = spark.read.text(data_file_path).filter(col('value').startswith(header_prefix)== True)
  header_record = df_read_header.withColumn('cutted', expr("substring(value,"+header_position+")")).first()
  #print("df_read_header: " + str(df_read_header))
  print("header_record: " + str(header_record))
  cnt=df_read_header.count()
  if cnt == 0:
    file_record_count= file_record_count
  if cnt !=0:
    header_record =str(header_record.cutted)
else:
  header_record = spark.sql(""" select 'No_Header' as cutted """)
  header_record=header_record.select("cutted").collect()
  header_record=header_record[0].cutted

if int(file_record_count) == int(record_count):
     print("Matching")  
else:
     print("MisMatch")

print("file_record_count:"+str(file_record_count))
print("record_count: " + str(record_count))
print("trailer_record :" + str(trailer_record))
print("header_record: " + str(header_record))


# COMMAND ----------

# DBTITLE 1,Cleanup the intermediate files from DBFS
# MAGIC %sh -e
# MAGIC if [ "$encryption_ind" == "Y" ]
# MAGIC then 
# MAGIC rm /dbfs/tmp/${work_file_name}
# MAGIC rm /dbfs/tmp/${work_file_name}_out
# MAGIC else 
# MAGIC rm /dbfs/tmp/$work_file_name
# MAGIC fi
# MAGIC 
# MAGIC RC=$?
# MAGIC if [ "$RC" != 0 ]
# MAGIC then 
# MAGIC echo "DBFS files clean failed" $RC
# MAGIC exit $RC
# MAGIC fi

# COMMAND ----------

# MAGIC %sh ls /dbfs/tmp/

# COMMAND ----------

# DBTITLE 1,Function to check the file presence:
def file_exists(input_file_path):
  try:
    dbutils.fs.ls(input_file_path)
    return True
  except Exception as e:
    return False

# COMMAND ----------

# DBTITLE 1,Function to validate the datetime:
from dateutil.parser import parse
def is_valid_date(date):
    if date:
        try:
            parse(date)
            return True
        except Exception as e:
            return False
    return False

# COMMAND ----------

# DBTITLE 1,Construct input file path:
input_file_path = ''

if spii_ind == 'Y':
  input_file_path="abfss://rvact-spii@{0}.dfs.core.windows.net/{1}/raw-spii/{2}/{3}/{4}".format(storage_account_name,env,file_prefix,SOURCE_DATA_DT,file_name)
  
elif pii_ind == 'Y':
  input_file_path="abfss://rvact-pii@{0}.dfs.core.windows.net/{1}/raw-pii/{2}/{3}/{4}".format(storage_account_name,env,file_prefix,SOURCE_DATA_DT,file_name)

else:
  input_file_path="abfss://rvact@{0}.dfs.core.windows.net/{1}/raw/{2}/{3}/{4}".format(storage_account_name,env,file_prefix,SOURCE_DATA_DT,file_name)
  
print(input_file_path)

# COMMAND ----------

# DBTITLE 1,The below cell does duplicate file check against the raw zone along with file name validates.
Duplicate=(str(file_exists(input_file_path)))

f_count=int(file_record_count)
t_count=int(record_count)

if Duplicate != "True":
  if len(str(source_tms)) != 14:
    #print("Source data timestamp is invalid")
    dbutils.notebook.exit("Invalid DateTime")
  else:
    print("Source data timestamp is valid")
    value=(str(is_valid_date(source_tms)))
    if value != "True":
        print("Invalid DateTime")
        dbutils.notebook.exit("Invalid DateTime")
    else:
        if int(file_record_count) != int(record_count):
          dbutils.notebook.exit("Mismatch: File Count-"+str(f_count)+" and Trailer Count-"+str(t_count))
          
        elif is_file_sequence != True:
          dbutils.notebook.exit(f"This file is out of order: {str(source_data_tms)}. Allowable files must be greater than: {str(struct_max_tms)}")
          
        else:
          #print("Valid")
          #dbutils.notebook.exit("Valid")
          dbutils.notebook.exit("Valid-Header Record and Sequence: "+str(header_record) + ";" + " Trailer Record: "+str(trailer_record)+";" + " Record Count: "+str(record_count)+";")
          #print(substring("Valid: Header Record-"+str(header_record.cutted)+" Trailer Record-"+str(trailer_record.cutted)+" Record Count: "+str(f_count),0,4))
else:
  #print('Already Exist')
  dbutils.notebook.exit("Already Exist")