import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("CI_CD_PySpark_Test") \
        .getOrCreate()

def test_spark_session_is_active(spark):
    assert spark is not None

def test_data_cleaning_logic(spark):
    mock_data = [("CUST01", None), ("CUST02", 50)]
    df_mock = spark.createDataFrame(mock_data, ["CustomerId", "Quantity"])

    df_cleaned = df_mock.withColumn(
        "Quantity", 
        when(col("Quantity").isNull(), 0).otherwise(col("Quantity"))
    )

    result = df_cleaned.filter(col("CustomerId") == "CUST01").collect()[0]

    assert result.Quantity == 0, "Lỗi: Logic xử lý Null không hoạt động!"