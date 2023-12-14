import os
import sys
import zipfile
import logging
from pyspark.sql import SparkSession

logging.basicConfig(filename='log.txt', level=logging.INFO)

def unzip_file(zip_filepath, extract_to):
    try:
        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
    except FileNotFoundError:
        logging.error(f"File {zip_filepath} does not exist.")
        print(f"File {zip_filepath} does not exist.", file=sys.stderr)
        sys.exit(-1)
    except zipfile.BadZipFile:
        logging.error(f"File {zip_filepath} is not a zip file.")
        print(f"File {zip_filepath} is not a zip file.", file=sys.stderr)
        sys.exit(-1)
    except Exception as e:
        logging.error(f"An error occurred while unzipping {zip_filepath}: {str(e)}")
        print(f"An error occurred while unzipping {zip_filepath}: {str(e)}", file=sys.stderr)
        sys.exit(-1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        logging.error("Usage: analysis <file1> <file2>")
        print("Usage: analysis <file1> <file2>", file=sys.stderr)
        sys.exit(-1)
    spark = None
    try:
        spark = SparkSession.builder.appName("PythonAnalysis").getOrCreate()

        # Unzip the files
        logging.info("Unzipping files...")
        unzip_file(sys.argv[1], '/tmp/real_estate') # Path should maybe be changed S3
        unzip_file(sys.argv[2], '/tmp/yelp') # Path should maybe be changed S3

        # Read the unzipped files
        logging.info("Reading unzipped files...")
        real_estate_df = spark.read.option("header", "true").csv('/tmp/real_estate/*.csv')
        yelp_df = spark.read.option("multiline", "true").json('/tmp/yelp')

        # Save the DataFrames in different formats
        logging.info("Saving DataFrames in different formats...")
        real_estate_df.write.json("s3a://spark-data/real_estate.json")
        real_estate_df.write.csv("s3a://spark-data/real_estate.csv")
        real_estate_df.write.format("avro").save("s3a://spark-data/real_estate.avro")

        yelp_df.write.json("s3a://spark-data/yelp.json")
        yelp_df.write.csv("s3a://spark-data/yelp.csv")
        yelp_df.write.format("avro").save("s3a://spark-data/yelp.avro")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print(f"An error occurred: {str(e)}", file=sys.stderr)
        sys.exit(-1)
    finally:
        if spark is not None:
            logging.info("Stopping Spark session...")
            spark.stop()