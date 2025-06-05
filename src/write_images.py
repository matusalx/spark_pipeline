from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os 
import pytesseract
from PIL import Image
import io
from pyspark.sql.functions import sha2, concat_ws, col, trim
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
from PIL import Image

import sys;print(sys.executable)

spark = SparkSession.builder \
        .master("local[*]") \
        .appName("DeltaLakeExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .getOrCreate()


parent_dir = parent_dir = os.path.dirname(os.path.abspath(__file__))

delta_lake_dir = os.path.join (parent_dir, "/tmp/delta-images")
image_dir = os.path.join (parent_dir, "/data_sample/Images")

merge_condition = "target.origin = source.origin"


records= []
for file in os.listdir(image_dir):
    path = os.path.join(image_dir, file)
    if os.path.isfile(path):
        with open(path, "rb") as f:
            raw = f.read()
            records.append((file, bytearray(raw)))


batches = [records[i:i+100] for i in range(0, len(records), 100)]

len(batches)
len(batches[51])



# df = spark.createDataFrame(records, ["filename", "image_bytes"])
# df = df.repartition(2000)
# batches= [batches[0],batches[1]]

schema = StructType([
    StructField("filename", StringType(), False),
    StructField("imagebytes", BinaryType(), False)
])
merge_condition = "target.filename = source.filename"


# to implement try catch here:
for i, batch in enumerate(batches):
    try:
        batch_df = spark.createDataFrame(batch, schema=schema)
        if not DeltaTable.isDeltaTable(spark, delta_lake_dir) or 1==1:
            batch_df.write.format("delta").mode("append").save(delta_lake_dir)
            print("###############################################")
            print("done writing for the first time iteration: ", i)
    except Exception as e:
        print(f"An error occurred: {e}")
    else:
        try:
            delta_table = DeltaTable.forPath(spark, delta_lake_dir)
            delta_table.alias("target").merge(
                source=batch_df.alias("source"),
                condition=merge_condition).whenMatchedUpdate(set={"imagebytes": "source.imagebytes"}) \
                .whenNotMatchedInsertAll() \
                .execute()
            print("###############################################")
            print("table merged/update with iteration:", i)
        except Exception as e:
            print(f"An error occurred: {e}")
spark.stop()
