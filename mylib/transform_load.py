'''
    Transform-Load
'''

from pyspark.sql.functions import col, year, month, dayofmonth

def transform(
    data_path="dbfs:/tmp/Spotify_2023.csv",  # Correct file path for Databricks DBFS
    spark=None
):
    """
    Transforms the data by renaming columns, adding date components, and calculating streams in millions.
    """
    if spark is None:
        raise ValueError("A Spark session must be provided.")

    # Load the data into a DataFrame
    df = spark.read.csv(data_path, header=True, inferSchema=True)

    # Rename columns to conform to naming conventions
    df = (
        df.withColumnRenamed("Track Name", "track_name")
        .withColumnRenamed("Artist Name", "artist_name")
        .withColumnRenamed("Released Date", "released_date")
    )

    # Add year, month, and day columns if 'released_date' is present
    if "released_date" in df.columns:
        df = (
            df.withColumn("released_year", year(col("released_date")))
            .withColumn("released_month", month(col("released_date")))
            .withColumn("released_day", dayofmonth(col("released_date")))
        )

    # Drop rows with missing data and calculate streams in millions
    df = df.dropna().withColumn("streams_millions", col("streams") / 1_000_000)
    return df


def load(
    df, 
    output_path="dbfs:/tmp/Spotify_Transformed.parquet",  # Correct DBFS path for Databricks
    file_format="parquet"
):
    """
    Saves the transformed DataFrame to the specified output path in the given format.
    """
    # Write the DataFrame in the specified format
    if file_format == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    elif file_format == "csv":
        df.write.mode("overwrite").option("header", True).csv(output_path)
    elif file_format == "json":
        df.write.mode("overwrite").json(output_path)
    else:
        raise ValueError("Unsupported file format. Choose 'parquet', 'csv', or 'json'.")

    return f"Data saved to {output_path} in {file_format} format."


# Example Usage in Databricks
if __name__ == "__main__":
    # Ensure Spark session is available
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Path where the file is saved from the extract step
    data_path = "dbfs:/tmp/Spotify_2023.csv"

    # Transform the dataset
    transformed_df = transform(data_path=data_path, spark=spark)

    # Show a preview of the transformed data
    transformed_df.show(5)

    # Save the transformed data
    output_path = "dbfs:/tmp/Spotify_Transformed.parquet"
    save_message = load(transformed_df, output_path=output_path, file_format="parquet")
    print(save_message)
