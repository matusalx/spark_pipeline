from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os 
import pytesseract
from PIL import Image
import io
from pyspark.sql.functions import sha2, concat_ws, col, trim
from pyspark.sql.types import StructType, StructField, StringType, BinaryType



import sys;print(sys.executable)

spark = SparkSession.builder \
        .master("local[*]") \
        .appName("DeltaLakeExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .getOrCreate()

delta_path = "/Users/vano/Desktop/spark_homowork/tmp/test-text-table"

from pyspark.sql.types import StructType


text_schema = StructType([
    StructField("text", StringType(), False),
])

text_dir = "/Users/vano/Desktop/spark_homowork/data_sample/Texts/Fake.csv"
delta_lake_dir = "/Users/vano/Desktop/spark_homowork/tmp/test-delta-table3"
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

##############################################################################

# Image writing 

# image_dir = "/Users/vano/Desktop/spark_homowork/data_sample/Images"
# df = spark.read.format("image").load(image_dir)
# df.count()
# df.first()
# df.printSchema()
# first_row = df.first()
# first_row["image"]["origin"]
# first_row["image"]["data"]
# columns_to_select = ["image.origin", "image.data"]
# df_selected = df.select(*columns_to_select)
# df_selected.printSchema()
# deduped_df = df_selected.dropDuplicates(["origin"])
# print("Number of texts dublicated and removed:", df_selected.count() - deduped_df.count())
# deduped_df = deduped_df.repartition(200)  # Adjust number based on cluster size and data volume
# deduped_df = deduped_df.first()


# delta_lake_dir = "/Users/vano/Desktop/spark_homowork/tmp/test-delta-images"
# merge_condition = "target.origin = source.origin"

# # if not DeltaTable.isDeltaTable(spark, delta_lake_dir):
# #     deduped_df.write.format("delta").mode("overwrite").save(delta_lake_dir)
# #     print("Delta table created. with name: ", delta_lake_dir)
# # else:
# #     delta_table = DeltaTable.forPath(spark, delta_lake_dir)

# #     delta_table.alias("target").merge(
# #             source=deduped_df.alias("source"),
# #             condition=merge_condition).whenMatchedUpdateAll() \
# #      .whenNotMatchedInsertAll() \
# #      .execute()
# #     print("Delta table merged/update with name:",delta_lake_dir )

# ######################
# # solve image writing :
# records= []
# for file in os.listdir(image_dir):
#     path = os.path.join(image_dir, file)
#     if os.path.isfile(path):
#         with open(path, "rb") as f:
#             raw = f.read()
#             records.append((file, bytearray(raw)))

# # len(records)
# # sys.getsizeof(records) 
# # sys.getsizeof(records) / (1024 ** 3)
# # print(records[0][0])

# batches = [records[i:i+100] for i in range(0, len(records), 100)]

# len(batches)
# len(batches[51])



# # df = spark.createDataFrame(records, ["filename", "image_bytes"])
# # df = df.repartition(2000)
# # batches= [batches[0],batches[1]]
# columns = ["filename", "iXmagebytes"]

# schema = StructType([
#     StructField("filename", StringType(), False),
#     StructField("imagebytes", BinaryType(), False)
# ])
# merge_condition = "target.filename = source.filename"


# for i, batch in enumerate(batches):
#     batch_df = spark.createDataFrame(batch, schema=schema)
#     if not DeltaTable.isDeltaTable(spark, delta_lake_dir):
#         batch_df.write.format("delta").mode("append").save(delta_lake_dir)
#         print("###############################################")
#         print("done writing for the first time iteration: ", i)
#     else:
#         delta_table = DeltaTable.forPath(spark, delta_lake_dir)
#         delta_table.alias("target").merge(
#             source=batch_df.alias("source"),
#             condition=merge_condition).whenMatchedUpdate(set={"imagebytes": "source.imagebytes"}) \
#             .whenNotMatchedInsertAll() \
#              .execute()
#         print("###############################################")
#         print("table merged/update with iteration:", i)

# spark.stop()
# del spark

# [ x[0] for x in batch if x[0] == '' or x[0] is  None]
# [ x[1] for x in batch if x[1] == '' or x[1] is  None]


# # delta_table.alias("target").merge(
# #     source_df.alias("source"),
# #     "target.filename = source.filename"
# # ).whenMatchedUpdate(set={
# #     "imagebytes": "source.imagebytes"  # must match source column exactly
# # }).whenNotMatchedInsert(values={
# #     "filename": "source.filename",
# #     "imagebytes": "source.imagebytes"
# # }).execute()“