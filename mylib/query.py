"""
Query Module
"""

# Import necessary libraries
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)
from pyspark.sql.functions import when, avg, col, count


# Define schema for the SpotifyDB table
schema = StructType(
    [
        StructField("music_id", IntegerType(), True),
        StructField("track_name", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_count", IntegerType(), True),
        StructField("released_year", IntegerType(), True),
        StructField("released_month", IntegerType(), True),
        StructField("released_day", IntegerType(), True),
        StructField("in_spotify_playlists", IntegerType(), True),
        StructField("in_spotify_charts", IntegerType(), True),
        StructField("streams", IntegerType(), True),
        StructField("in_apple_playlists", IntegerType(), True),
        StructField("in_apple_charts", IntegerType(), True),
        StructField("in_deezer_playlists", IntegerType(), True),
        StructField("in_deezer_charts", IntegerType(), True),
        StructField("in_shazam_charts", IntegerType(), True),
        StructField("bpm", IntegerType(), True),
        StructField("key", StringType(), True),
        StructField("mode", StringType(), True),
        StructField("danceability_percent", FloatType(), True),
        StructField("valence_percent", FloatType(), True),
        StructField("energy_percent", FloatType(), True),
        StructField("acousticness_percent", FloatType(), True),
        StructField("instrumentalness_percent", FloatType(), True),
        StructField("liveness_percent", FloatType(), True),
        StructField("speechiness_percent", FloatType(), True),
    ]
)


# Define a Spotify DataFrame Manager Class
class SpotifyDataFrameManager:
    def __init__(self, spark_session):
        """
        Initialize the SpotifyDataFrameManager with a Spark session.
        """
        self.spark = spark_session
        # Initialize DataFrame placeholder with the defined schema
        self.spotify_df = self.spark.createDataFrame([], schema)

    def query_create(self):
        """
        Create a new record in the Spotify DataFrame.
        """
        new_data = [
            (
                12345,
                "Sample Track",
                "Sample Artist",
                3,
                2023,
                8,
                15,
                100,
                50,
                50000000,
                200,
                180,
                150,
                100,
                50,
                120,
                "A",
                "Minor",
                80.5,
                70.4,
                90.2,
                12.3,
                0.0,
                15.6,
                5.8,
            )
        ]
        new_record_df = self.spark.createDataFrame(new_data, schema)
        self.spotify_df = self.spotify_df.union(new_record_df)
        return "Create Success"

    def query_read(self, limit=5):
        """
        Read and display a limited number of records from the Spotify DataFrame.
        """
        self.spotify_df.show(limit)
        return "Read Success"

    def query_update(self, record_id=12345, new_artist_name="Updated Artist"):
        """
        Update the artist name for a specific record by music_id.
        """
        self.spotify_df = self.spotify_df.withColumn(
            "artist_name",
            when(self.spotify_df.music_id == record_id, new_artist_name).otherwise(
                self.spotify_df.artist_name
            ),
        )
        return "Update Success"

    def query_delete(self, record_id=12345):
        """
        Delete a record from the Spotify DataFrame by music_id.
        """
        self.spotify_df = self.spotify_df.filter(self.spotify_df.music_id != record_id)
        return "Delete Success"

    def query_read_by_year(self, year):
        """
        Retrieve records from the Spotify DataFrame by release year.

        Args:
            year (int): The release year to filter records.

        Returns:
            str: Success message with record count.
        """
        result_df = self.spotify_df.filter(self.spotify_df.released_year == year)
        record_count = result_df.count()
        result_df.show()
        return f"Read by Year Success: {record_count} records found"

    def query_average_streams(self):
        """
        Calculate the average streams across all records in the Spotify DataFrame.
        """
        avg_streams = self.spotify_df.agg(avg("streams")).first()[0]
        return avg_streams if avg_streams is not None else 0

    def query_top_tracks(self, limit=5):
        """
        Retrieve the top tracks by stream count.

        Args:
            limit (int): Number of top tracks to retrieve.

        Returns:
            str: Success message with record count.
        """
        top_tracks_df = self.spotify_df.orderBy(col("streams").desc()).limit(limit)
        top_tracks_df.show()
        return f"Top {limit} tracks retrieved successfully."

    def query_streams_by_artist(self, artist_name):
        """
        Calculate the total streams for a specific artist.

        Args:
            artist_name (str): The name of the artist.

        Returns:
            int: Total streams for the specified artist.
        """
        total_streams = (
            self.spotify_df.filter(self.spotify_df.artist_name == artist_name)
            .agg(count("streams"))
            .first()[0]
        )
        return total_streams or 0


# Example Usage in Databricks
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    # Create Spark session
    spark = SparkSession.builder.getOrCreate()

    # Initialize the Spotify DataFrame Manager
    spotify_manager = SpotifyDataFrameManager(spark)

    # Example operations
    print(spotify_manager.query_create())  # Add a new record
    print(spotify_manager.query_read())  # Read and display records
    print(spotify_manager.query_update())  # Update an artist's name
    print(spotify_manager.query_delete())  # Delete a record
    print(spotify_manager.query_read_by_year(2023))  # Query records by year
    print(f"Average Streams: {spotify_manager.query_average_streams()}")  # Avg streams
    print(spotify_manager.query_top_tracks(limit=3))  # Top 3 tracks
    print(
        f"Total Streams for 'Sample Artist': "
        f"{spotify_manager.query_streams_by_artist('Sample Artist')}"
    )
