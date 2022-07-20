# Databricks notebook source
# DBTITLE 1,Importing pyspark libraries:
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

# DBTITLE 1,Get widget values:
#dbutils.widgets.removeAll()

#dbutils.widgets.text("feed_name", "")
feed_name = dbutils.widgets.get("feed_name")

#dbutils.widgets.text("filename", "")
file_name = dbutils.widgets.get("filename")

#dbutils.widgets.text("env", "")
env = dbutils.widgets.get("env")

#dbutils.widgets.text("adb_par","")
adb_par_dict = json.loads(dbutils.widgets.get("adb_par"))

# COMMAND ----------

# DBTITLE 1,Validates widget values:
try:
  file_name
except NameError:
  print("file_name is blank")
else:
  print("File Name :" +file_name)

try:
  env
except NameError:
  print("env value is blank")
else:
  print("Env :" + env)
  

try:
  adb_par_dict
except NameError:
  print("adb_par is blank")
else:
  print("ADB Parameter :" +str(adb_par_dict))

# COMMAND ----------

# DBTITLE 1,Parse & Assign values to variables:

file_prefix = file_name[:file_name.rindex('_')-0]
SOURCE_DATA_DT = file_name[file_name.rindex('_')+1:file_name.rindex('_')+1+8]
SOURCE_DATA_TMS = file_name[file_name.rindex('_')+1:file_name.rindex('_')+1+14]
source_data_tms = file_name[file_name.rindex('_')+1:file_name.rindex('_')+1+14]

print("Feed Name :" +feed_name)
print("File Prefix :" +file_prefix)
print("SOURCE_DATA_DT :" +SOURCE_DATA_DT)
print("SOURCE_DATA_TMS :" +SOURCE_DATA_TMS)
print("source_data_tms :"+source_data_tms)

# COMMAND ----------

# DBTITLE 1,Parse ADLS Configuration Settings from JSON dictonary variable:
l_storage_account_name = adb_par_dict["local_storage_act"]
service_principle_clientid = adb_par_dict["service_principle_clientid"]
service_principle_directory_id = adb_par_dict["service_principle_directory_id"]
databricks_scopename = adb_par_dict["databricks_scopename"]
keyvault_secret_name = adb_par_dict["keyvault_secret_name"]
teradata_secret_name = adb_par_dict["teradata_secret_name"]
eventhub_connection_string = adb_par_dict["eventhub_connection_string"]
gpgkey_secret_name = adb_par_dict["gpgkey_secret_name"]

spark.conf.set("fs.azure.account.auth.type." + l_storage_account_name + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + l_storage_account_name + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + l_storage_account_name + ".dfs.core.windows.net", service_principle_clientid)
spark.conf.set("fs.azure.account.oauth2.client.secret." + l_storage_account_name + ".dfs.core.windows.net", dbutils.secrets.get(scope = databricks_scopename, key = keyvault_secret_name))
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + l_storage_account_name + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + service_principle_directory_id + "/oauth2/token")

print("local_storage_account_name = " + l_storage_account_name)
print("service_principle_clientid = " + service_principle_clientid)
print("service_principle_directory_id = " + service_principle_directory_id)
print("databricks_scopename = " + databricks_scopename)

# COMMAND ----------

# DBTITLE 1,Read a paramter file:
parameters = sqlContext.read.option('multiline','true').json("abfss://config@"+str(l_storage_account_name)+".dfs.core.windows.net/job_param.json")

# COMMAND ----------

# DBTITLE 1,Pasre the paramter file and populate variables:
#app_id = parameters.select(col('app_id')).collect()[0]['app_id']
azure_env = parameters.select(col('azure_env')).collect()[0]['azure_env']
adls_storage_cntner = parameters.select(col('adls_storage_cntner')).collect()[0]['adls_storage_cntner']
adls_storage_cntner_pii = parameters.select(col('adls_storage_cntner_pii')).collect()[0]['adls_storage_cntner_pii']
adls_storage_cntner_spii=parameters.select(col('adls_storage_cntner_spii')).collect()[0]['adls_storage_cntner_spii']
mosaic_env = parameters.select(col('mosaic_env')).collect()[0]['mosaic_env']
mosaic_uid = parameters.select(col('mosaic_uid')).collect()[0]['mosaic_uid']
fail_email_to = parameters.select(col('fail_email_to')).collect()[0]['fail_email_to']
succ_email_to = parameters.select(col('succ_email_to')).collect()[0]['succ_email_to']
reject_email_to = parameters.select(col('reject_email_to')).collect()[0]['reject_email_to']
adls_storage_act = parameters.select(col('adls_storage_act')).collect()[0]['adls_storage_act']
file_type=(parameters.select(feed_name+'_feed_info.file_type').collect()[0]['file_type'])
header_ind =(parameters.select(feed_name+'_feed_info.header_ind').collect()[0]['header_ind'])
trailer_ind =(parameters.select(feed_name+'_feed_info.trailer_ind').collect()[0]['trailer_ind'])
header_prefix =(parameters.select(feed_name+'_feed_info.header_prefix').collect()[0]['header_prefix'])
header_parsing_ind =(parameters.select(feed_name+'_feed_info.header_parsing_ind').collect()[0]['header_parsing_ind'])
trailer_prefix =(parameters.select(feed_name+'_feed_info.trailer_prefix').collect()[0]['trailer_prefix'])
trailer_parsing_ind =(parameters.select(feed_name+'_feed_info.trailer_parsing_ind').collect()[0]['trailer_parsing_ind'])
struct_table =(parameters.select(feed_name+'_feed_info.struct_table').collect()[0]['struct_table'])
prep_table =(parameters.select(feed_name+'_feed_info.prep_table').collect()[0]['prep_table'])
struct_partition_col =(parameters.select(feed_name+'_feed_info.struct_partition_col').collect()[0]['struct_partition_col'])
#prep_partition_col =(parameters.select(feed_name+'_feed_info.prep_partition_col').collect()[0]['prep_partition_col'])
data_file_extn=(parameters.select(feed_name+'_feed_info.data_file_extn').collect()[0]['data_file_extn'])
# header_filter =(parameters.select('YQYR_feed_info.header_filter').collect()[0]['header_filter'])
# trailer_filter =(parameters.select('YQYR_feed_info.trailer_filter').collect()[0]['trailer_filter'])
mosaic_load_ind =(parameters.select(feed_name+'_feed_info.mosaic_load_ind').collect()[0]['mosaic_load_ind'])
count_audit_check_ind =(parameters.select(feed_name+'_feed_info.count_audit_check_ind').collect()[0]['count_audit_check_ind'])
#newly added 10/09/2020
header_position =(parameters.select(feed_name+'_feed_info.header_position').collect()[0]['header_position'])
trailer_position =(parameters.select(feed_name+'_feed_info.trailer_position').collect()[0]['trailer_position'])
trailer_rec_count_start=(parameters.select(feed_name+'_feed_info.trailer_rec_count_start').collect()[0]['trailer_rec_count_start'])
trailer_rec_count_end=(parameters.select(feed_name+'_feed_info.trailer_rec_count_end').collect()[0]['trailer_rec_count_end'])
prep_load_ind=(parameters.select(feed_name+'_feed_info.prep_load_ind').collect()[0]['prep_load_ind'])
#newly added 10/27/2020
trailer_count_match_value=(parameters.select(feed_name+'_feed_info.trailer_count_match_value').collect()[0]['trailer_count_match_value'])
encryption_ind=(parameters.select(feed_name+'_feed_info.encryption_ind').collect()[0]['encryption_ind'])
#newly added 02/09/2021
# reload_struct_ind=(parameters.select(feed_name+'_feed_info.reload_struct_ind').collect()[0]['reload_struct_ind'])
# file_frequency = (parameters.select(feed_name+'_feed_info.file_frequency').collect()[0]['file_frequency'])
# file_reoccurence = (parameters.select(feed_name+'_feed_info.file_reoccurence').collect()[0]['file_reoccurence'])
pii_ind=(parameters.select(feed_name+'_feed_info.pii_ind').collect()[0]['pii_ind'])
spii_ind=(parameters.select(feed_name+'_feed_info.spii_ind').collect()[0]['spii_ind'])
file_frequency_check_ind=(parameters.select(feed_name+'_feed_info.file_frequency_check_ind').collect()[0]['file_frequency_check_ind'])
raw_container=(parameters.select(feed_name+'_feed_info.raw_container').collect()[0]['raw_container'])
raw_folder=(parameters.select(feed_name+'_feed_info.raw_folder').collect()[0]['raw_folder'])


if file_type == "fixedwidth":
  columns=(parameters.select(feed_name+'_feed_info.columns').collect()[0]['columns'])
  width=(parameters.select(feed_name+'_feed_info.width').collect()[0]['width'])
  print("Columns :" + str(columns))
  print("Width :" + str(width))

if file_type == "delimited":
  file_header=(parameters.select(feed_name+'_feed_info.file_header').collect()[0]['file_header'])
  delimiter=(parameters.select(feed_name+'_feed_info.delimiter').collect()[0]['delimiter'])
  struct_columns=(parameters.select(feed_name+'_feed_info.struct_columns').collect()[0]['struct_columns'])
  print("file_header :" + str(file_header))
  print("delimiter :" + str(delimiter))
  print("struct_columns :" + str(struct_columns))
  
  
if mosaic_load_ind == "Y":
  mosaic_wrk_table=(parameters.select(feed_name+'_feed_info.mosaic_wrk_table').collect()[0]['mosaic_wrk_table'])
  mosaic_db=mosaic_env+'_LZ_DB'

if pii_ind == "Y":
  pii_struct_attributes=(parameters.select(feed_name+'_feed_info.pii_struct_attributes').collect()[0]['pii_struct_attributes'])
  pii_prep_attributes=(parameters.select(feed_name+'_feed_info.pii_prep_attributes').collect()[0]['pii_prep_attributes'])

if spii_ind == "Y":
  spii_struct_attributes=(parameters.select(feed_name+'_feed_info.spii_struct_attributes').collect()[0]['spii_struct_attributes'])
  spii_prep_attributes=(parameters.select(feed_name+'_feed_info.spii_prep_attributes').collect()[0]['spii_prep_attributes'])

if file_frequency_check_ind == "Y":
  file_frequency =(parameters.select(feed_name+'_feed_info.file_frequency').collect()[0]['file_frequency'])
  file_reoccurence=(parameters.select(feed_name+'_feed_info.file_reoccurence').collect()[0]['file_reoccurence'])
  
if encryption_ind == "Y":
  private_key_file_name = (parameters.select(feed_name+'_feed_info.private_key_file_name').collect()[0]['private_key_file_name'])
  
  
print("file_type :" + file_type)
print("Azure Env :" + azure_env)
print("Adls Sorage Container :" + adls_storage_cntner)
print("Adls Sorage Container SPII:" + adls_storage_cntner_spii)
print("Adls Sorage Container PII:" + adls_storage_cntner_pii)
print("Mosaic Teradata Env :" + mosaic_env)
print("Mosaic Teradata User Id :" + mosaic_uid)
print("Adls Storage Account :" + adls_storage_act)
print("Header Indicator :" + header_ind)
print("Trailer Indicator :" + str(trailer_ind))
print("header_prefix :" + str(header_prefix))
print("trailer_prefix :" + str(trailer_prefix))
print("header_parsing_ind :" + str(header_parsing_ind))
print("trailer_parsing_ind :" + str(trailer_parsing_ind))
print("mosaic_load_ind :" + str(mosaic_load_ind))
print("struct_table :" + str(struct_table))
print("prep_table :" + str(prep_table))
print("struct_partition_col :" + str(struct_partition_col))
#print("prep_partition_col :" + str(prep_partition_col))
print("data_file_extn :" + str(data_file_extn))
print("count_audit_check_ind :" + str (count_audit_check_ind))
print("header_position :" + str(header_position))
print("trailer_position :" + str(trailer_position))
print("trailer_rec_count_start :" + str(trailer_rec_count_start))
print("trailer_rec_count_end :" + str(trailer_rec_count_end))
print("trailer_count_match_value :" + str(trailer_count_match_value))
print("prep_load_ind :" + str(prep_load_ind))
print("encryption_ind :" + str(encryption_ind))

print("fail_email_to :" + str(fail_email_to))
print("succ_email_to :" + str(succ_email_to))
print("reject_email_to :" + str(reject_email_to))
print("pii_ind :" + str(pii_ind))
print("spii_ind :" + str(spii_ind))
print("file_frequency_check_ind :" + str(file_frequency_check_ind))
if file_frequency_check_ind == "Y":  
  print("file_frequency :" + file_frequency)
  print("file_reoccurence :" + file_reoccurence)
if pii_ind == "Y":
  listToStr = ', '.join([str(i) for i in pii_struct_attributes])
  print("pii_struct_attributes :" + listToStr)
  listToStr = ', '.join([str(i) for i in pii_prep_attributes])
  print("pii_prep_attributes :" + listToStr)
if spii_ind == "Y":
  listToStr = ', '.join([str(i) for i in spii_struct_attributes])
  print("spii_struct_attributes :" + listToStr)
  listToStr = ', '.join([str(i) for i in spii_prep_attributes])
  print("spii_prep_attributes :" + listToStr)
print("raw_container :" + raw_container)
print("raw_folder :" + raw_folder)

if encryption_ind == "Y":
  print("private_key_file_name :" + private_key_file_name)

# COMMAND ----------

# DBTITLE 1, End of Notebook Run
