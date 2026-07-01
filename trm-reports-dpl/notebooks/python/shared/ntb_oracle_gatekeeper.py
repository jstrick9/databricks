# Databricks notebook source
dbutils.widgets.text("dbx_env","dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/"+dbutils.widgets.get("dbx_env")+"/"+config_file_name

print(f'{config_file=},{dbx_env=}')

# COMMAND ----------

# MAGIC %run ./ntb_common_func_and_params $config_file=config_file 

# COMMAND ----------

common_configs = read_yaml(config_file)

# COMMAND ----------

common_configs = read_yaml(config_file)
edw_scope = common_configs['secrets']['edw_scope']

# COMMAND ----------



def oracle_is_ready(edw_scope) -> bool: 
    """ Checks if Oracle database is ready by attempting a simple query. Returns True if the probe succeeds, otherwise False. """ 
    try: 
        edw_probe = "SELECT 2 FROM dual" 
        df = read_data_from_oracle_conn_dsu_cmn(edw_probe, edw_scope) 
        if df is not None and not df.isEmpty():
            print("Oracle database probe successful") 
            return True 
        else: 
            print("Oracle probe returned empty result")
    except Exception as e:
        raise f"Oracle connection check failed: {e}"

# COMMAND ----------

# Call the oracle_is_ready function and display the result
is_ready = oracle_is_ready(edw_scope)
print( "Oracle is Ready True or False?  ",is_ready)