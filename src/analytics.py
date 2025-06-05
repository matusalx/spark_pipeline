from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import sha2, concat_ws, col, trim, explode
from pyspark.sql.types import StructType, StructField, StringType, BinaryType
import os

spark = SparkSession.builder \
        .master("local[*]") \
        .appName("DeltaLakeExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .getOrCreate()



parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

delta_image_results = os.path.join (parent_dir, "tmp/image_results")
delta_text_results = os.path.join (parent_dir, "tmp/text_results")


df_image_results = spark.read.format("delta").load(delta_image_results)
df_text_results = spark.read.format("delta").load(delta_text_results)

df_image_results.createOrReplaceTempView("image_results")
df_text_results.createOrReplaceTempView("text_results")

category_ratio = spark.sql("""
                    SELECT 
                    "Images" as source,
                    category,  
                    total_benign_count,
                    count( distinct filename) as category_count,
                    round(category_count / total_benign_count * 100, 2) || "%"  as category_ratio
                    FROM image_results 
                    left join (select count(*) as total_benign_count from image_results where category = "Unknown")
                    where category != "Unknown"       
                    group by category,total_benign_count
                           
                    UNION ALL 
                           
                    SELECT 
                    "Text" as source,
                    category,  
                    total_benign_count,
                    count( distinct title) as category_count,
                    round(category_count / total_benign_count * 100, 2) || "%"  as category_ratio
                    FROM text_results 
                    left join (select count(*) as total_benign_count from text_results where category = "Unknown")
                    where category != "Unknown"       
                    group by category,total_benign_count
                    """)


category_ratio.show()

# use len(title) % 364 as a day, or  len(filename) % 364 to imitate the days

daily_count_of_threats  = spark.sql("""
                    select 
                    "Images" as source,
                    category, 
                    len(filename) % 364 as day,                 
                    count(filename) as num_count
                    from image_results
                    group by category,day

                    UNION ALL 
                                      
                    select 
                    "Text" as source,
                    category, 
                    len(title) % 364 as day,                
                    count(title) as num_count
                    from text_results
                    group by category,day
                    order by num_count desc
                    """)

daily_count_of_threats.show()

# for latest events we use alphabetical order, instead of real timestamps

latest_event_for_category = spark.sql("""
                    select 
                    "Images" as source,
                    category, 
                    max(filename) as last_file_received
                    from image_results
                    group by category

                    UNION ALL 
                                      
                    select 
                    "Text" as source,
                    category, 
                    max(title) as last_file_received
                    from text_results
                    group by category
                    """)

latest_event_for_category.show()


spark.stop()
print("Spark stopped")