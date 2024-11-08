from mylib.extract import extract
from mylib.transform_load import transform, load
from mylib.query import query_create, query_read, query_update, query_delete
from pyspark.sql import SparkSession

def main():
    # Start a Spark session
    spark = SparkSession.builder.appName("SpotifyETL").getOrCreate()

    # Step 1: Extract the CSV file
    extract_path = extract()

    # Step 2: Transform the data
    transformed_df = transform(extract_path, spark)
    
    # Step 3: Load the data into storage (e.g., Parquet)
    load_path = "output/Spotify_Transformed.parquet"
    load(transformed_df, output_path=load_path, file_format="parquet")

    # Step 4: Execute query functions
    print(query_create(spark))  # Insert record
    print(query_read(spark))    # Read records
    print(query_update(spark))  # Update record
    print(query_delete(spark))  # Delete record

    # Return main results for verification
    results = {
        "extract_to": extract_path,
        "transform_db": load_path,
        "create": query_create(spark),
        "read": query_read(spark),
        "update": query_update(spark),
        "delete": query_delete(spark),
    }
    
    # Stop the Spark session
    spark.stop()
    
    return results

# Run main function if this is the main script
if __name__ == "__main__":
    main()
