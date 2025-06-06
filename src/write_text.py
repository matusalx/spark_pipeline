from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
from PIL import Image
from pyspark.sql.functions import sha2, concat_ws, col, trim, regexp_replace, col, lower
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


# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parent_dir= "/Users/vano/Desktop/spark_homowork"
delta_lake_dir = os.path.join (parent_dir, "tmp/delta_text")

text_dir = os.path.join (parent_dir, "data_sample/Texts/Fake.csv")

df = spark.read.csv(text_dir, header=True, inferSchema=True)

df_with_hash = df.withColumn("row_hash", sha2(concat_ws("||", *df.columns), 256))

# df_with_hash.show()
# df.show()


deduped_df = df_with_hash.dropDuplicates(["row_hash"])
print("Number of texts dublicated and removed:", df_with_hash.count() - deduped_df.count())


deduped_df.printSchema()

schema = StructType([
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("date", StringType(), True),
    StructField("row_hash", StringType(), True)
])

# deduped_df = spark.createDataFrame(deduped_df.rdd, schema=schema)

deduped_df.printSchema()


merge_condition = "target.row_hash = source.row_hash"

# if not exists then crate or else insert/update
# use partition while creating the table
# use hash to reduce date text lenght (long text cannot be used as a partition)
deduped_df = deduped_df.withColumn("date", sha2(col("date"), 256))

# after repartiion there is no warnong for heap space
deduped_df = deduped_df.repartition(200)


if not DeltaTable.isDeltaTable(spark, delta_lake_dir):
    try:
        deduped_df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("date") \
            .save(delta_lake_dir)
        print("Delta table created. with name: ", delta_lake_dir)
    except Exception as e:
        print(f"An error occurred: {e}")
else:
    try:
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
    except Exception as e:
        print(f"An error occurred: {e}")    

spark.stop()