import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import regexp_extract, col, to_timestamp, year, month, dayofmonth, hour, when

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']

pattern = r'^(\S+) (\S+) (\S+) \[([^\]]+)\] "(\S+) (.*?) (\S+)" (\d{3}) (\S+) "([^"]*)" "([^"]*)"'

print("Reading JSON files from S3...")
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path], "recurse": True},
    format="json",
    transformation_ctx="read_json_logs"  
)

df = dynamic_frame.toDF()
print(f"Total records read: {df.count()}")

df = df.select("raw_line")

df = df.select(
    regexp_extract(col("raw_line"), pattern, 1).alias("host"),
    regexp_extract(col("raw_line"), pattern, 2).alias("ident"),
    regexp_extract(col("raw_line"), pattern, 3).alias("user"),
    regexp_extract(col("raw_line"), pattern, 4).alias("ts_str"),
    regexp_extract(col("raw_line"), pattern, 5).alias("method"),
    regexp_extract(col("raw_line"), pattern, 6).alias("path"),
    regexp_extract(col("raw_line"), pattern, 7).alias("protocol"),
    regexp_extract(col("raw_line"), pattern, 8).alias("status"),
    regexp_extract(col("raw_line"), pattern, 9).alias("bytes_str"),
    regexp_extract(col("raw_line"), pattern, 10).alias("referrer"),
    regexp_extract(col("raw_line"), pattern, 11).alias("user_agent")
)


df = df.withColumn("status", col("status").cast("int"))
df = df.withColumn("bytes",
    when(col("bytes_str") == "-", None)
    .otherwise(col("bytes_str").cast("bigint"))
)


df = df.withColumn("ts", to_timestamp(col("ts_str"), "dd/MMM/yyyy:HH:mm:ss Z"))


df = df.withColumn("year", year(col("ts")).cast("string"))
df = df.withColumn("month", month(col("ts")).cast("string"))
df = df.withColumn("day", dayofmonth(col("ts")).cast("string"))
df = df.withColumn("hour", hour(col("ts")).cast("string"))

final_df = df.select(
    "host", "ident", "user", "ts_str", "method", "path", "protocol",
    "status", "referrer", "user_agent", "ts", "bytes",
    "year", "month", "day", "hour"
)

print(f"Parsed records: {final_df.count()}")
print("Sample parsed data:")
final_df.show(5, truncate=False)

output_dyf = DynamicFrame.fromDF(final_df, glueContext, "output")

print("Writing Parquet to S3 with partitioning...")
glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["year", "month", "day", "hour"]
    },
    format="parquet",
    transformation_ctx="write_parquet_logs"  #Required!
)

print("Glue job completed")
job.commit()
