'''
    Main
'''
from mylib.extract import extract, load_data
from mylib.transform_load import transform, load
from mylib.query import SpotifyDataFrameManager
from pyspark.sql import SparkSession

def main():
    # Start a Spark session
    spark = SparkSession.builder.appName("SpotifyETL").getOrCreate()

    # Step 1: Extract the CSV file
    extract_path = extract()

    # Step 2: Transform the data
    transformed_df = transform(data_path=extract_path, spark=spark)

    # Step 3: Load the data into storage
    load_path = "dbfs:/tmp/Spotify_Transformed.parquet"
    load(transformed_df, output_path=load_path, file_format="parquet")

    # Step 4: Initialize Spotify DataFrame Manager
    manager = SpotifyDataFrameManager(spark)

    # CRUD Operations
    create_status = manager.query_create()
    read_status = manager.query_read()
    update_status = manager.query_update()
    delete_status = manager.query_delete()

    # Additional Queries
    read_by_year_status = manager.query_read_by_year(2023)
    average_streams = manager.query_average_streams()

    # Collect results
    results = {
        "extract_to": extract_path,
        "transform_db": load_path,
        "create": create_status,
        "read": read_status,
        "update": update_status,
        "delete": delete_status,
        "read_by_year": read_by_year_status,
        "average_streams": average_streams,
    }

    print("Results dictionary before returning:", results)

    return results

if __name__ == "__main__":
    main()
