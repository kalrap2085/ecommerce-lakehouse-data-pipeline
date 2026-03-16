from pyspark.sql.functions import current_timestamp, lit, expr

def log_dq_checks(spark, full_table_name: str, condition: str, error_message: str, error_cnt: int, error_type: str, source_file: str) :

    _dq_table = "gold.dq_results"
    
    _cols = ["table_name", "condition", "error_message", "failed_count", "error_type", "source_file"]
    
    _data = [(full_table_name, condition, error_message, error_cnt, error_type, source_file)]
    
    df = spark.createDataFrame(data = _data, schema = _cols) \
              .withColumn("failed_count", expr("CAST(failed_count as int)")) \
              .withColumn("error_id", expr("uuid()")) \
              .withColumn("created_on", current_timestamp()) \
              .withColumn("created_by", lit("dq_logging.py"))
        
    df.write.format("delta").mode("append").saveAsTable(_dq_table)
    
    print(f"SPARK-APP: DQ - [{error_type}] : {full_table_name} | {error_message} | Failed count : {error_cnt}")