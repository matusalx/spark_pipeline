from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, col, trim, explode

from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os 
import pytesseract
from PIL import Image

import io



import sys;print(sys.executable)

# spark = SparkSession.builder.getOrCreate()

# spark = SparkSession.builder.appName("MyApp").getOrCreate()

# spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


# print(spark.version)


# spark = SparkSession.builder \
#         .appName("DeltaLakeExample") \
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#         .getOrCreate()


spark = SparkSession.builder \
        .appName("DeltaLakeExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .getOrCreate()


# spark = configure_spark_with_delta_pip(spark).getOrCreate()

data = [("Alice", 25), ("Bob", 30), ("Cathy", 27)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)

delta_path = "/Users/vano/Desktop/spark_homowork/tmp/test-delta-table"

df.write.format("delta").mode("overwrite").save(delta_path)

df2 = spark.read.format("delta").load(delta_path)

df2.show()
df2.printSchema()

##############################################################################

# read sample picture and write to delta lake
# /Users/vano/Desktop/spark_homowork/data_sample/Images/sign_text.png
image_df = spark.read.format("image").load("/Users/vano/Desktop/spark_homowork/data_sample/Images/sign_text.png")

image_df.printSchema()
image_df.select("image.origin", "image.height", "image.width", "image.nChannels", "image.mode").show(5, truncate=False)

image_df.write.format("delta").mode("overwrite").save("/Users/vano/Desktop/spark_homowork/tmp/images")

df = spark.read.format("delta").load("/Users/vano/Desktop/spark_homowork/tmp/images")

# write by using bytearray to store raw picture

records = []
image_dir = "/Users/vano/Desktop/spark_homowork/data_sample/Images"
write_dir =  "/Users/vano/Desktop/spark_homowork/tmp/images"

for file in os.listdir(image_dir):
    path = os.path.join(image_dir, file)
    if os.path.isfile(path):
        with open(path, "rb") as f:
            raw = f.read()
            records.append((file, bytearray(raw)))

df = spark.createDataFrame(records, ["filename", "image_bytes"])
df.write.format("delta").mode("overwrite").save(write_dir)


#read from delta lake
df = spark.read.format("delta").load(write_dir)
image_rows = df.collect()

image_rows[0][1]

# Convert bytearray to a file-like object
image_stream = io.BytesIO(image_rows[0][1])

# Open image using PIL
image = Image.open(image_stream)

# OCR
text = pytesseract.image_to_string(image)



################################
# read and write json data to delta lake

delta_path_category_mapping = "/Users/vano/Desktop/spark_homowork/tmp/test_category_table"

category_mapping_dir = "/Users/vano/Desktop/spark_homowork/category_mapping.json"

df = spark.read.option("multiline", "true").json(category_mapping_dir)

df.write.format("delta").mode("overwrite").save(delta_path_category_mapping)

df = spark.read.format("delta").load(delta_path_category_mapping)

df.show()
df.printSchema()



df = spark.read.format("delta").load(delta_path_category_mapping)
# Flatten category and keywords
keywords_df = df.selectExpr("explode(threat_categories) as threat") \
                .select(col("threat.category").alias("category"),
                        explode(col("threat.keywords")).alias("keyword"))

keyword_category_map = keywords_df.select("keyword", "category") \
                                   .rdd.map(lambda row: (row["keyword"].lower(), row["category"])) \
                                   .collect()

 # to try configure_spark_with_delta_pip ??

broadcast_map = spark.sparkContext.broadcast(dict(keyword_category_map))


def classify_text(text):
    text_lower = text.lower()
    for keyword, category in broadcast_map.value.items():
        if keyword in text_lower:
            return category
    return "Unknown"

spark.stop()
del spark