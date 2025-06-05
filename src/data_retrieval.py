import kagglehub
from kagglehub import KaggleDatasetAdapter
from requests.exceptions import ConnectTimeout,ReadTimeout
import shutil
import time
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, col, trim, explode



import sys;print(sys.executable)

# Download the text dataset

path = kagglehub.dataset_download(
    "clmentbisaillon/fake-and-real-news-dataset"
)

source_path = path + '/Fake.csv'
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
destination_path = os.path.join(parent_dir, "data_sample/Texts")

shutil.copy(source_path, destination_path)

print(f"Dataset copied to: {destination_path}")

# Download the image dataset 1
try:
    path_images = kagglehub.dataset_download(
    "cassandrapratt/childrens-book-covers-with-captions")
except (ConnectTimeout, ReadTimeout):
    print("ConnectTimeout")
    time.sleep(10)
    path_images = kagglehub.dataset_download(
    "cassandrapratt/childrens-book-covers-with-captions")



source_path_images = path_images + "/childrens-books"
destination_path_images = os.path.join (parent_dir, "data_sample/Images")


image_names = os.listdir(source_path_images)

for fname in image_names:
    if fname not in os.listdir(destination_path_images):
        shutil.copy(os.path.join(source_path_images, fname), destination_path_images)



# Download the image dataset 2


try:
    path_images_2 = kagglehub.dataset_download(
    "suvroo/scanned-images-dataset-for-ocr-and-vlm-finetuning")
except (ConnectTimeout, ReadTimeout):
    print("ConnectTimeout Exeption Occured, sleeping for 10 seconds")
    time.sleep(10)
    path_images_2 = kagglehub.dataset_download(
    "suvroo/scanned-images-dataset-for-ocr-and-vlm-finetuning")

image_folder_names = ["ADVE", "Email", "Form", "Letter", "Memo", "News", "Note", "Report", "Resume", "Scientific"]


source_path_images_2 = [path_images_2 + "/dataset/" + x for x in image_folder_names ]


for dir_name in source_path_images_2:
    for x in os.listdir(dir_name):
        if fname not in os.listdir(destination_path_images):
             shutil.copy(os.path.join(dir_name, x), destination_path_images)

print("Finished writing Images")


#######################################################
# Write category mapping into delta lake

spark = SparkSession.builder \
        .appName("DeltaLakeExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.1") \
        .getOrCreate()



delta_path_category_mapping = os.path.join(parent_dir, "tmp/category_table")
category_mapping_dir = os.path.join(parent_dir, "category_mapping.json")


df = spark.read.option("multiline", "true").json(category_mapping_dir)
df.write.format("delta").mode("overwrite").save(delta_path_category_mapping)

spark.stop()
print("Finished writing catogery mapping")
