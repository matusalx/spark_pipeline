from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import os 
import pytesseract
from PIL import Image
import io
from pyspark.sql.functions import sha2, concat_ws, col, trim, explode
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
import pytesseract

import sys;print(sys.executable)

spark = SparkSession.builder \
        .master("local[*]") \
        .appName("DeltaLakeExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .getOrCreate()

############################################################
# Get category mappings

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

delta_path_category_mapping = os.path.join (parent_dir, "tmp/category_table")


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


############################################################
# read and clasify images


delta_lake_dir = os.path.join (parent_dir, "tmp/delta_images")
write_image_results = os.path.join (parent_dir, "tmp/image_results")

df = spark.read.format("delta").load(delta_lake_dir)
# test = df.first()
# test["filename"]
# tesseract_results = []

for row in df.limit(59).toLocalIterator():
    # io.BytesIO(row["imagebytes"])
    image = Image.open(io.BytesIO(row["imagebytes"]))
    tesseract_text = pytesseract.image_to_string(image)
    classified_text = classify_text(tesseract_text)
    # tesseract_results.append((row["filename"],classified_text))
    # image_results = spark.createDataFrame(tesseract_results, ["filename", "category"])
    image_results = spark.createDataFrame([(row["filename"],classified_text)], ["filename", "category"])
    image_results.write.format("delta").mode("overwrite").save(write_image_results)



    print("Finished filename: ", row["filename"])


# image_results = spark.createDataFrame(tesseract_results, ["filename", "category"])
# image_results.write.format("delta").mode("overwrite").save(write_image_results)

spark.stop()

