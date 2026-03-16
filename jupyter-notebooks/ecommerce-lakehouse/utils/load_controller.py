from pyspark.sql.functions import max, current_timestamp, lit, to_timestamp,col
from delta.tables import DeltaTable

#Insert data to load_controller
def insert_control_logs(spark, data: list) -> bool :
    try:
        
        _schema = ["layer", "schema_name", "table_name", "load_type", "write_mode", "load_timestamp", "load_status", "loaded_count", "created_by", "modified_by"]
    
        df = spark.createDataFrame(data = data, schema = _schema)
    
        df_ld = df.withColumn("load_timestamp", to_timestamp(col("load_timestamp"))) \
                  .withColumn("created_on", current_timestamp()) \
                  .withColumn("modified_on", current_timestamp())
                
        df_ld.write.format("delta").mode("append").saveAsTable("gold.load_controller")
        return True
    
    except Exception as e:
        print(f"SPARK-APP: Error while inserting the entry to load_controller : {e}")
        return False

#Get the max load_timestamp from load_controller for a given table 
def get_max_loadTimestamp(spark, schema_name : str, table_name : str) -> str :
    try:
        
        row = spark.read.table("gold.load_controller") \
             .where(f"schema_name = '{schema_name}' and table_name = '{table_name}'") \
             .agg(max("load_timestamp").alias("max_load_timestamp")) \
             .first()
         
        return '1900-01-01 00:00:00.000000' if row is None or row.max_load_timestamp is None else row.max_load_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")     
        
    except Exception as e:
        print(f"SPARK-APP: Error while fetching the max_load_timestamp from load_controller for Schema = {schema_name}, Table = {table_name} : {e}")
        return None
         

#Delete the data from load_controller table for full loads
def delete_control_logs(spark, schema_name : str, table_name : str) -> bool :
    try:
        
        dt = DeltaTable.forName(spark, "gold.load_controller")
        
        dt.delete(f"schema_name = '{schema_name}' and table_name = '{table_name}'")
                              
        return True
    
    except Exception as e:
        print(f"SPARK-APP: Error while deleting data from load_controller for Schema = {schema_name}, Table = {table_name} : {e}")
        return False
    
#Truncate the load_controller table
def truncate_control_logs(spark) -> bool :
    try:
        
        dt = DeltaTable.forName(spark, "gold.load_controller")
        
        #Disbale retention safety check
        spark.sql("SET spark.databricks.delta.retentionDurationCheck.enabled=false")
        
        #Delete all rows
        dt.delete()
        
        #Vacuum old files
        dt.vacuum(0)
        
        return True
    
    except Exception as e:
        print(f"SPARK-APP: Error while truncating load_controller : {e}")
        return False
    
