from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import when  # Import when for conditional updates

# Initialize Spark session
spark = SparkSession.builder.appName("SpotifyDBOperations").getOrCreate()

# Define schema for the SpotifyDB table
schema = StructType([
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
    StructField("speechiness_percent", FloatType(), True)
])

# Initialize an empty DataFrame for SpotifyDB
spotify_df = spark.createDataFrame([], schema)

# Function to insert a new record
def query_create():
    new_data = [(12345, "Sample Track", "Sample Artist", 3, 2023, 8, 15, 100, 50, 50000000, 
                 200, 180, 150, 100, 50, 120, "A", "Minor", 80.5, 70.4, 90.2, 12.3, 0.0, 15.6, 5.8)]
    new_record_df = spark.createDataFrame(new_data, schema)
    
    global spotify_df
    spotify_df = spotify_df.union(new_record_df)
    return "Create Success"

# Function to read records with a limit
def query_read(limit=5):
    global spotify_df
    spotify_df.show(limit)
    return "Read Success"

# Function to update a specific record
def query_update(record_id=12345, new_artist_name="Updated Artist"):
    global spotify_df
    spotify_df = spotify_df.withColumn(
        "artist_name",
        when(spotify_df.music_id == record_id, new_artist_name).otherwise(spotify_df.artist_name)
    )
    return "Update Success"

# Function to delete a specific record
def query_delete(record_id=12345):
    global spotify_df
    spotify_df = spotify_df.filter(spotify_df.music_id != record_id)
    return "Delete Success"

# Test the functions
if __name__ == "__main__":
    print(query_create())    # Insert a new record
    print(query_read())      # Read records
    print(query_update())    # Update a record
    print(query_delete())    # Delete a record

# Stop the Spark session
spark.stop()
