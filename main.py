'''
    Main
'''

from mylib.extract import extract
from mylib.transform_load import transform, load
from mylib.query import SpotifyDataFrameManager
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

    # Step 4: Initialize the Spotify DataFrame Manager and execute query functions
    manager = SpotifyDataFrameManager(spark)

    # CRUD Operations and Read by Year
    print(manager.query_create())
    print(manager.query_read())
    print(manager.query_update())
    print(manager.query_delete())
    
    # Query records by year (e.g., 2023) and calculate average streams
    print(manager.query_read_by_year(2023))
    average_streams = manager.query_average_streams()
    print(f"Average Streams: {average_streams}")

    # Collect results for summary report
    results = {
        "extract_to": extract_path,
        "transform_db": load_path,
        "create": manager.query_create(),
        "read": manager.query_read(),
        "update": manager.query_update(),
        "delete": manager.query_delete(),
        "read_by_year": manager.query_read_by_year(2023),
        "average_streams": average_streams,
    }

    # Step 5: Generate Markdown Report
    with open("summary_report.md", "w") as file:
        file.write("# Spotify Data Analysis Summary\n\n")
        file.write("This is summary of the Spotify dataset analysis.\n\n")

        # Adding average streams information
        file.write("## Average Streams\n")
        file.write(
            "The average number of streams across all records is "
            f"**{average_streams:.2f}**.\n\n"
        )

        # Adding a sample output for each query operation
        file.write("## CRUD Operation Results\n")
        file.write(f"- **Create Record**: {results['create']}\n")
        file.write(f"- **Read Records**: {results['read']}\n")
        file.write(f"- **Update Record**: {results['update']}\n")
        file.write(f"- **Delete Record**: {results['delete']}\n\n")

        # Adding year-specific data
        file.write("## Records for the Year 2023\n")
        file.write(f"{results['read_by_year']}\n")

    # Stop the Spark session
    spark.stop()

    return results

# Run main function if this is the main script
if __name__ == "__main__":
    main()
