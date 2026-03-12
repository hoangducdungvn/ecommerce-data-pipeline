from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, month

def run_pipeline():
    # 1. Khởi tạo Spark
    spark = SparkSession.builder \
        .appName("Ecommerce_Data_Pipeline") \
        .config("spark.jars.packages", "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11") \
        .getOrCreate()

    # 2. BRONZE LAYER (Ingestion)
    jdbc_url = "jdbc:sqlserver://192.168.10.104:1433;databaseName=MiniRecommendation;encrypt=true;trustServerCertificate=true;"
    props = {"user": "sa", "password": "hoangducdung", "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"}
    
    df_bronze = spark.read.jdbc(url=jdbc_url, table="CustomerOrderSummary", properties=props)
    df_bronze.write.mode("overwrite").parquet("/opt/airflow/data/bronze/customer_orders")

    # 3. SILVER LAYER (Clean & Optimize)
    df_cleaned = df_bronze.filter(col("CustomerId").isNotNull()) \
        .withColumn("OrderYear", year(col("LineItemOrdering"))) \
        .withColumn("OrderMonth", month(col("LineItemOrdering")))
    
    df_cleaned.write.mode("overwrite").partitionBy("OrderYear", "OrderMonth").parquet("/opt/airflow/data/silver/customer_orders")

    # 4. GOLD LAYER (Data Mart)
    dim_customer = df_cleaned.select("CustomerId", "CustomerName").distinct()
    dim_customer.write.mode("overwrite").parquet("/opt/airflow/data/gold/dim_customer")
    
    print("✅ Hoàn thành toàn bộ Pipeline!")

if __name__ == "__main__":
    run_pipeline()