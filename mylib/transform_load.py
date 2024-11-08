from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth

# Initialize a Spark session
spark = SparkSession.builder.appName("SpotifyDataETL").getOrCreate()

# Define the transformation function
def transform(data_path="data/Spotify_Most_Streamed_Songs.csv"):
    # Load the CSV file into a PySpark DataFrame
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    
    # Example transformations:
    # 1. Rename columns to snake_case if needed
    df = df.withColumnRenamed("Track Name", "track_name") \
           .withColumnRenamed("Artist Name", "artist_name") \
           .withColumnRenamed("Released Date", "released_date")

    # 2. Extract year, month, and day from a date column if it exists
    if "released_date" in df.columns:
        df = df.withColumn("released_year", year(col("released_date"))) \
               .withColumn("released_month", month(col("released_date"))) \
               .withColumn("released_day", dayofmonth(col("released_date")))

    # 3. Filter out rows with missing values (optional)
    df = df.dropna()

    # 4. Add any other transformations needed (e.g., change data types, add calculated columns)
    df = df.withColumn("streams_millions", col("streams") / 1_000_000)

    return df

# Define the load function
def load(df, output_path="output/Spotify_Transformed.parquet", file_format="parquet"):
    # Save the transformed DataFrame to the specified format and path
    if file_format == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    elif file_format == "csv":
        df.write.mode("overwrite").option("header", True).csv(output_path)
    elif file_format == "json":
        df.write.mode("overwrite").json(output_path)
    else:
        raise ValueError("Unsupported file format. Choose 'parquet', 'csv', or 'json'.")

    return f"Data saved to {output_path} in {file_format} format."

if __name__ == "__main__":
    # Run transform and load functions
    transformed_df = transform()
    print(load(transformed_df))

# Stop the Spark session
spark.stop()
