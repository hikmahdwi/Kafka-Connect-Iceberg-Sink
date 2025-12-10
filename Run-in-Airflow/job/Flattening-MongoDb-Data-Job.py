from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce, get_json_object
from pyspark.sql.types import TimestampType, DateType, StructField, StructType, StringType, LongType

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

HIVE_METASTORE_URI = "thrift://hive-metastore:9083"
WAREHOUSE_PATH = "s3a://iceberg-test/warehouse/"
TABLE_NAME = "spark_catalog.db_test.`iceberg_flat_redeem`"

spark = SparkSession.builder \
    .appName("Falttened Redeem & Insert Into Iceberg") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.spark_catalog.uri", HIVE_METASTORE_URI) \
    .config("spark.sql.catalog.spark_catalog.warehouse", WAREHOUSE_PATH) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.memoryFraction", "0.8") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .config("spark.driver.memory", "1g") \
    .enableHiveSupport() \
    .getOrCreate()

#-------------------------- Read Data From Hive Metastore ------------------------------  

print("=========================================")
print("===== [DEBUG] FIRST DATA INPUT =====")
print("=========================================")

def get_last_processed_commit(target_table_name):
    try:
        high_watermark_df = spark.sql(f"""SELECT MAX(ts_ms) FROM {target_table_name}""")
        high_watermark_result = high_watermark_df.first()[0]
        return high_watermark_result if high_watermark_result is not None else 0

    except Exception as e:
        print("=========================================================================")
        print(f"===== [WARNING] FALSE TO READ COMMIT TIME FROM {target_table_name} =====")
        print("=========================================================================")
        print(f"MESSAGE ERROR: {e}")
        print(f"THE FIRST RUNNING WILL BE ERROR BECAUSE THE TABLE NOT EXIST YET")
        return 0

high_watermark = get_last_processed_commit(TABLE_NAME)
print(f"===== [INFO] LAST PROCESSED COMMIT ts_ms: {high_watermark} =====")

df_sql = spark.sql(f"""SELECT * FROM spark_catalog.db_test.redeem_logs WHERE ts_ms > {high_watermark} Order by ts_ms Desc Limit 2000""")

# df_sql.cache()
print("=========================================")
print("==== EXTRACT AND FLATTENING THE DATA ====")  
print("=========================================")

json_schema = StructType([
    StructField("_id", StructType([
        StructField("$oid", StringType(), True)
    ]), True),
    StructField("requested_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("outlet_code", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("sender_id", StringType(), True),
    StructField("createdAt", StructType([
        StructField("$date", LongType(), True)
    ]), True),
    StructField("updatedAt", StructType([
        StructField("$date", LongType(), True)
    ]), True),
    StructField("metadata", StructType([
        StructField("coupon", StringType(), True),
        StructField("data", StringType(), True)
    ]), True)
])

def transform_redeem_data(df_sql, schema):
    # if df_sql.isEmpty():
    #     print("=======================================================================")
    #     print("==== [INFO] INCREMENTAL DATA IS EMPTY. SKIPPING THE TRANSORMATION. ====")
    #     print("=======================================================================")
    #     return None
    
    print("=========================================")  
    print("====== [DEBUG] SCHEMA WILL BE USED ======")
    print("=========================================")

    df_with_json = df_sql.withColumn("json_data", coalesce(col("after"), col("before")))
    df_parsed = df_with_json.withColumn("parsed_data", from_json(col("json_data"), schema))

    df_flattened = df_parsed.select(
        "ts_ms",
        "op",
        col("parsed_data._id.$oid").alias("oid"),
        col("parsed_data.requested_id").alias("requested_id"),
        col("parsed_data.campaign_id").alias("campaign_id"),
        col("parsed_data.outlet_code").alias("outlet_code"),
        col("parsed_data.phone_number").alias("phone_number"),
        col("parsed_data.sender_id").alias("sender_id"),
        col("parsed_data.createdAt.$date").alias("createdAt_long"),
        col("parsed_data.updatedAt.$date").alias("updatedAt_long"),
        col("parsed_data.metadata.coupon").alias("coupon"),
        col("parsed_data.metadata.data").alias("description"),
        "json_data" 
    )

    df_final = df_flattened \
        .withColumn("created_at_ts", (col("createdAt_long") / 1000).cast(TimestampType())) \
        .withColumn("updated_at_ts", (col("updatedAt_long") / 1000).cast(TimestampType())) \
        .withColumn("partition_date", ((col("ts_ms") / 1000).cast(TimestampType())).cast(DateType())) \
        .drop("createdAt_long", "updatedAt_long") 
    df_final = df_final.sortWithinPartitions("partition_date")
    # df_final.cache()

    return df_final

df_trans_result = transform_redeem_data(df_sql, json_schema)

# df_sql.unpersist()

if df_trans_result is not None:
    print("=========================================")
    print("==== [DEBUG] WRITING TO ICEBERG TABLE ===")  
    print("=========================================")
    # df_trans_result.show(10)
    write_options = {
        "write.distribution-mode": "none",
        "write.parquet.compression-codec": "zstd",
        "wirte.metadata.compression-codec": "gzip"
    }

    try:
        # Check if table exists
        if high_watermark != 0 :
            print(f"Table {TABLE_NAME} exists — appending data...")
            df_trans_result.writeTo(TABLE_NAME) \
            .option("merge-schema", "true") \
            .append()
            
        else:
            print(f"Table {TABLE_NAME} does not exist — creating it...")
            df_trans_result.writeTo(TABLE_NAME) \
            .option("merge-schema", "true") \
            .create()
        
        print("=========================================")
        print("==== [DEBUG] WRITING TO ICEBERG TABLE ===")  
        print("=========================================")

    except Exception as e:
        print("==============================================")
        print("==== [ERROR] FAILED TO WRITE ICEBERG TABLE ===")  
        print("==============================================")
        print(f"MESSAGE ERROR: {e}")
        raise
    finally:
        df_trans_result.unpersist()

else:
    print("=============================================")
    print("==== [INFO] NO DATA TO WRITE TO ICEBERG =====")  
    print("=============================================")
# df_his = spark.sql("SELECT * FROM spark_catalog.iceberg_db.redeem_logs.history")
# df_his.show(10)
spark.stop()
