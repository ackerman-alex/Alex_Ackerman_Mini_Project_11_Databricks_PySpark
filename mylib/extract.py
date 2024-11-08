from pyspark.sql import SparkSession
import requests
import os

# Initialize a Spark session
spark = SparkSession.builder.appName("ExtractSpotifyDataset").getOrCreate()

def extract(
    url="https://raw.githubusercontent.com/RunCHIRON/dataset/refs/heads/main/Spotify_2023.csv",
    file_path="data/Spotify_Most_Streamed_Songs.csv",
    timeout=10,  # Adding a timeout to avoid indefinite hanging
):
    """Extract a URL to a file path"""

    # Ensure the 'data' directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Extract the file from the URL with a timeout
    with requests.get(url, timeout=timeout) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    return file_path

def load_data(file_path):
    """Load data into a PySpark DataFrame"""
    # Read the CSV file into a Spark DataFrame
    return spark.read.csv(file_path, header=True, inferSchema=True)

if __name__ == "__main__":
    # Extract the dataset
    file_path = extract()

    # Load it into a Spark DataFrame
    spotify_df = load_data(file_path)

    # Show the DataFrame schema and first few rows (for verification)
    spotify_df.printSchema()
    spotify_df.show(5)
    
    # Stop the Spark session
    spark.stop()
