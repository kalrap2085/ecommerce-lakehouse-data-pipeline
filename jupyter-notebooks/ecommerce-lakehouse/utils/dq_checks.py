from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, current_timestamp
from utils.dq_logging import log_dq_checks

def dq_validate_checks (df: DataFrame, spark, schema: str, table_name: str, source_file: str) -> DataFrame :
    
    _error_conditions = []
    _full_table_name = f"{schema}.{table_name}"
    _bad_table = f"bad_{table_name}"
    
    checks_fact_orders = [
        ("order_id IS NULL", "order_id is NULL", "ERROR"),
        ("line_num IS NULL", "line_num is NULL", "ERROR"),
        ("order_date IS NULL", "order_date is NULL", "ERROR"),
        ("ship_date IS NOT NULL AND ship_date < order_date", "ship_date is before order_date", "ERROR"),
        ("customer_id IS NULL", "customer_id is NULL", "ERROR"),
        ("store IS NULL", "store is NULL", "ERROR"),
        ("channel IS NULL", "channel is NULL", "ERROR"),
        ("currency IS NULL", "currency is NULL", "ERROR"),
        ("product_id IS NULL", "product_id is NuLL", "ERROR"),
        ("quantity <= 0", "quantity must be > 0", "ERROR"),
        ("unit_price < 0", "unit_price is negative", "ERROR"),
        ("list_price < 0", "list_price is negative", "ERROR"),
        ("order_status IS NULL", "order_status is NULL", "ERROR"),
        ("payment_status IS NULL", "payment_status is NULL", "ERROR")
    ]

    checks_fact_payments = [
        ("payment_id IS NULL", "payment_id is NULL", "ERROR"),
        ("payment_date IS NULL", "payment_date is NULL", "ERROR"),
        ("order_id IS NULL", "order_id is NULL", "ERROR"),
        ("customer_id IS NULL", "customer_id is NULL", "ERROR"),
        ("store IS NULL", "store is NULL", "ERROR"),
        ("channel IS NULL", "channel is NULL", "ERROR"),
        ("currency IS NULL", "currency is NULL", "ERROR"),
        ("payment_status IS NULL", "payment_status is NULL", "ERROR"),
        ("payment_method IS NULL", "payment_method is NULL", "WARN"),
        ("amount < 0", "payment amount is negative", "ERROR"),
        ("is_refund = true AND refund_amount <= 0", "refund_amount must be > 0 when is_refund is true", "ERROR"),
        ("is_refund = false AND refund_amount > 0", "refund_amount present when is_refund is false", "ERROR")
    ]
    
    checks_fact_inventory = [
        ("inventory_date IS NULL", "inventory_date is NULL", "ERROR"),
        ("product_id IS NULL", "product_id is NULL", "ERROR"),
        ("store IS NULL", "store is NULL", "ERROR"),
        ("channel IS NULL", "channel is NULL", "ERROR"),
        ("currency IS NULL", "currency is NULL", "ERROR"),
        ("cost_price < 0", "cost_price is negative", "ERROR"),
        ("quantity_on_hand < 0", "quantity_on_hand is negative", "ERROR"),
        ("quantity_reserved < 0", "quantity_reserved is negative", "ERROR"),
        ("quantity_available < 0", "quantity_available is negative", "ERROR"),
        ("quantity_reserved > quantity_on_hand", "quantity_reserved > quantity_on_hand", "ERROR"),
        ("quantity_available > quantity_on_hand", "quantity_available > quantity_on_hand", "ERROR")
    ]
    
    checks_fact_ads_performance = [
        ("campaign_id IS NULL", "campaign_id is NULL", "ERROR"),
        ("ads_date IS NULL", "ads_date is NULL", "ERROR"),
        ("store IS NULL", "store is NULL", "ERROR"),
        ("channel IS NULL", "channel is NULL", "ERROR"),
        ("currency IS NULL", "currency is NULL", "ERROR"),
        ("impressions < 0", "impressions is negative", "ERROR"),
        ("clicks < 0", "clicks is negative", "ERROR"),
        ("cost < 0", "cost is negative", "ERROR"),
        ("orders_generated < 0", "orders_generated is negative", "ERROR"),
        ("revenue_generated < 0", "revenue_generated is negative", "ERROR"),
        ("clicks > impressions", "clicks greater than impressions", "ERROR"),
        ("orders_generated > clicks", "orders_generated greater than clicks", "ERROR")
    ]
    
    checks_fact_customer_events = [
        ("event_id IS NULL", "event_id is NULL", "ERROR"),
        ("customer_id IS NULL", "customer_id is NULL", "ERROR"),
        ("store IS NULL", "store is NULL", "ERROR"),
        ("channel IS NULL", "channel is NULL", "ERROR"),
        ("product_id IS NULL", "product_id is NuLL", "ERROR")
    ]
    
    checks_dim_customer = [
        ("customer_id IS NULL", "customer_id is NULL", "ERROR"),
        ("store IS NULL", "store is NULL", "ERROR"),
        ("channel IS NULL", "channel is NULL", "ERROR"),
        ("effective_start_date IS NULL", "effective_start_date is NULL", "ERROR"),
        ("is_current IS NULL", "is_current is NULL", "ERROR"),
        ("is_deleted IS NULL", "is_deleted is NULL", "ERROR"),
        ("is_deleted = true AND is_current = true", "Record cannot be deleted and current", "ERROR"),
        ("gender IS NOT NULL AND gender NOT IN ('M','F','O')", "Invalid gender value", "WARN"),
        ("date_of_birth IS NOT NULL AND date_of_birth > current_date()", "DOB in future", "ERROR")
    ]

    checks_dim_customer_address = [
        ("address_id IS NULL", "address_id is NULL", "ERROR"),
        ("customer_id IS NULL", "customer_id is NULL", "ERROR"),
        ("store IS NULL", "store is NULL", "ERROR"),
        ("channel IS NULL", "channel is NULL", "ERROR"),
        ("address_type IS NULL", "address_type is NULL", "WARN"),
        ("effective_start_date IS NULL", "effective_start_date is NULL", "ERROR"),
        ("is_current IS NULL", "is_current is NULL", "ERROR"),
        ("is_deleted = true AND is_current = true", "Record cannot be deleted and current", "ERROR"),
        ("zip IS NULL", "zip code missing", "WARN"),
        ("country IS NULL", "country missing", "WARN")
    ]

    checks_dim_product = [
        ("product_id IS NULL", "product_id is NULL", "ERROR"),
        ("product_name IS NULL", "product_name is NULL", "WARN"),
        ("category IS NULL", "category is NULL", "WARN"),
        ("product_status IS NULL", "product_status is NULL", "ERROR"),
        ("is_deleted IS NULL", "is_deleted is NULL", "ERROR"),
        ("discontinued_date IS NOT NULL AND launch_date IS NOT NULL AND discontinued_date < launch_date",
         "discontinued_date before launch_date", "ERROR")
    ]
    
    checks_dim_product_price = [
        ("product_id IS NULL", "product_id is NULL", "ERROR"),
        ("store IS NULL", "store is NULL", "ERROR"),
        ("channel IS NULL", "channel is NULL", "ERROR"),
        ("currency IS NULL", "currency is NULL", "ERROR"),
        ("list_price < 0", "list_price is negative", "ERROR"),
        ("cost_price < 0", "cost_price is negative", "ERROR"),
        ("effective_start_date IS NULL", "effective_start_date is NULL", "ERROR"),
        ("is_current IS NULL", "is_current is NULL", "ERROR"),
        ("is_deleted = true AND is_current = true", "Record cannot be deleted and current", "ERROR")
    ]
    
    checks_table_mapping = {
        "gold.fact_orders" : checks_fact_orders,
        "gold.fact_payments" : checks_fact_payments,
        "gold.fact_inventory" : checks_fact_inventory,
        "gold.fact_ads_performance" : checks_fact_ads_performance,
        "gold.dim_customer" : checks_dim_customer,
        "gold.dim_customer_address" : checks_dim_customer_address,
        "gold.dim_product" : checks_dim_product,
        "gold.dim_product_price" : checks_dim_product_price,
        "gold.fact_customer_events" : checks_fact_customer_events
    }
    
    if _full_table_name not in checks_table_mapping:
        print(f"No DQ checks defined for table {_full_table_name}. Skipping validation.")
        return
    else :    
        _checks = checks_table_mapping[_full_table_name]
    
    for _condition, _error_message, _error_type in _checks:
        
        df_filter = df.where(_condition)
        _error_cnt = df_filter.count()
        
        if _error_cnt > 0:
            log_dq_checks(spark, _full_table_name, _condition, _error_message, _error_cnt, _error_type, source_file)
            
            if _error_type == "ERROR":
                _error_conditions.append(_condition)
                
                df_filter \
                   .withColumn("error_description", lit(_error_message)) \
                   .withColumn("created_on", current_timestamp()) \
                   .withColumn("created_by", lit("dq_checks.py")) \
                   .withColumn("modified_on", current_timestamp()) \
                   .withColumn("modified_by", lit("dq_checks.py")) \
                   .write \
                   .format("delta") \
                   .mode("append") \
                   .saveAsTable(f"bad.{_bad_table}")
                
           
    if _error_conditions:
        df_valid = df.where(f"NOT ({' OR '.join(_error_conditions)})")
    else:
        df_valid = df
        
    return df_valid