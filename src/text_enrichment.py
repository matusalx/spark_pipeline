from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import sha2, concat_ws, col, trim, explode
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
import os


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

broadcast_map = spark.sparkContext.broadcast(dict(keyword_category_map))

def classify_text(text):
    text_lower = text.lower()
    for keyword, category in broadcast_map.value.items():
        if keyword in text_lower:
            return category
    return "Unknown"


############################################################

# read, clasify text and write results
delta_lake_dir = os.path.join (parent_dir, "tmp/delta_text")
write_text_results = os.path.join (parent_dir, "tmp/text_results")

df = spark.read.format("delta").load(delta_lake_dir)

text_results = []
for row in df.toLocalIterator():
    text = row["title"]
    classified_text = classify_text(text)
    text_results.append((row["title"],classified_text, row["row_hash"]))

    print("Finished hash: ", row["row_hash"])


text_results = spark.createDataFrame(text_results, ["title", "category", "row_hash"])
text_results.write.format("delta").mode("overwrite").save(write_text_results)

spark.stop()
