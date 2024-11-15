'''
    Extact
'''

import requests
from pyspark.sql.functions import col

def extract(
    url="https://raw.githubusercontent.com/RunCHIRON/dataset/refs/heads/main/Spotify_2023.csv",
    file_path="/dbfs/tmp/Spotify_2023.csv",  # Save the file temporarily in DBFS
    timeout=10,
):
    """
    Downloads a file from a specified URL and saves it to the given file path.
    """
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()  # Raise an error for bad status codes
    with open(file_path, "wb") as file:
        file.write(response.content)
    return file_path


def load_data(file_path):
    """
    Loads data from a CSV file into a PySpark DataFrame.
    """
    return spark.read.csv("dbfs:/tmp/Spotify_2023.csv", header=True, inferSchema=True)


# Extract the dataset
file_path = extract()

# Load it into a Spark DataFrame
spotify_df = load_data(file_path)

# Rename columns to remove special characters
spotify_df = spotify_df.select(
    [col(c).alias(c.replace("(", "").replace(")", "").replace(" ", "_").replace("-", "_")) for c in spotify_df.columns]
)

# Show the updated schema and a few rows
spotify_df.printSchema()
spotify_df.show(5)

# Write the DataFrame as a table in the Databricks catalog
spotify_df.write.mode("overwrite").saveAsTable("ids706_data_engineering.default.Spotify_2023")

