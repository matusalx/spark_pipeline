from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
from PIL import Image
from pyspark.sql.functions import sha2, concat_ws, col, trim
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
from pyspark.sql.types import StructType
import os



import sys;print(sys.executable)

spark = SparkSession.builder \
        .master("local[*]") \
        .appName("DeltaLakeExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .getOrCreate()


parent_dir = parent_dir = os.path.dirname(os.path.abspath(__file__))
delta_lake_dir = os.path.join (parent_dir, "/tmp/text-table")

text_dir = os.path.join (parent_dir, "/data_sample/Texts/Fake.csv")

df = spark.read.csv(text_dir, header=True, inferSchema=True)

df_with_hash = df.withColumn("row_hash", sha2(concat_ws("||", *df.columns), 256))

# df_with_hash.show()
# df.show()


deduped_df = df_with_hash.dropDuplicates(["row_hash"])
print("Number of texts dublicated and removed:", df_with_hash.count() - deduped_df.count())

# deduped_df.filter(col("date").isNull()).show()
# deduped_df = deduped_df.filter(col("date").isNotNull())
# deduped_df.select("date").distinct().show()
# deduped_df.filter(col("date").isNull()  | (trim(col("date")) == "" )).show()
# deduped_df.filter(col("date").isNull()).count()
# deduped_df = deduped_df.withColumn("date", col("deduped_df").cast("string"))

deduped_df.printSchema()

schema = StructType([
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("date", StringType(), True),
    StructField("row_hash", StringType(), True)
])
deduped_df = spark.createDataFrame(deduped_df.rdd, schema=schema)

deduped_df.printSchema()


merge_condition = "target.row_hash = source.row_hash"

# if not exists then crate or else insert/update
# use partition while creating the table
delta_table = DeltaTable.forPath(spark, delta_lake_dir)


if not DeltaTable.isDeltaTable(spark, delta_lake_dir):
    deduped_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("date") \
        .save(delta_lake_dir)
    print("Delta table created. with name: ", delta_lake_dir)
else:
    delta_table = DeltaTable.forPath(spark, delta_lake_dir)

    delta_table.alias("target").merge(
            source=deduped_df.alias("source"),
            condition=merge_condition).whenMatchedUpdate(
                set={
        "row_hash": "source.row_hash"
                }).whenNotMatchedInsert( 
                values={
        "row_hash": "source.row_hash"
                }).execute()
    print("Delta table merged/update with name:",delta_lake_dir )


spark.stop()