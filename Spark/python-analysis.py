import sys
import zipfile
from pyspark.sql import SparkSession

def unzip_file(zip_filepath, extract_to):
    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        zip_ref.extractall(extract_to)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: analysis <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("PythonAnalysis").getOrCreate()

    # Unzip the files
    unzip_file(sys.argv[1], '/tmp/real_estate')
    unzip_file(sys.argv[2], '/tmp/yelp')

    # Read the unzipped files
    real_estate_df = spark.read.option("header", "true").csv('/tmp/real_estate/*.csv')
    yelp_df = spark.read.option("header", "true").json('/tmp/yelp/*.json')

    # Save the DataFrames in different formats
    real_estate_df.write.json("s3a://spark-data/real_estate.json")
    real_estate_df.write.csv("s3a://spark-data/real_estate.csv")
    real_estate_df.write.format("avro").save("s3a://spark-data/real_estate.avro")

    yelp_df.write.json("s3a://spark-data/yelp.json")
    yelp_df.write.csv("s3a://spark-data/yelp.csv")
    yelp_df.write.format("avro").save("s3a://spark-data/yelp.avro")

    spark.stop()