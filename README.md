[![CI](https://github.com/nogibjj/Alex_Ackerman_Mini_Project_10_PySpark/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/Alex_Ackerman_Mini_Project_10_PySpark/actions/workflows/cicd.yml)

# Alex Ackerman Mini Project 10 - PySpark
Welcome to the Alex Ackerman Mini Project 10 - PySpark repository! This project showcases the use of PySpark for managing a Spotify dataset, performing data operations, and analyzing various music-related metrics.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Functions](#functions)
5. [Dataset Schema](#dataset-schema)
6. [Example CRUD Operations](#example-crud-operations)
7. [Anlaysis Queries in query.py](#analysis-queries-in-query.py)
8. [Contributing](#contributing)
9. [License](#license)

## Project Overview
This project demonstrates how to utilize PySpark to manage and analyze music streaming data effectively. The dataset includes information about songs, artists, playlists, and streaming statistics across platforms like Spotify, Apple Music, Deezer, and Shazam. It supports basic CRUD (Create, Read, Update, Delete) operations using PySpark DataFrames, enabling efficient data processing for large datasets.

## Installation
1. Clone the 
```bash
git clone https://github.com/nogibjj/Alex_Ackerman_Mini_Project_10_PySpark.git
cd Alex_Ackerman_Mini_Project_10_PySpark
```
2. Install Required Packages
    - This project requires PySpark
```bash
pip install pyspark
```
3. Set Up the Environment
Ensure that you have a compatible environment with Spark installed. Refer to the [official Spark documentation](https://spark.apache.org/downloads.html) for system-specific setup.

## Usage
1. Start PySpark in your terminal or IDE.
2. Load the Dataset as a DataFrame by running the provided code snippets.
3. Run CRUD Operations using the provided function definitions in the script.

This project enables you to explore the dataset and perform various transformations and analyses. Modify or extend the code to fit specific needs as you explore the data.

## Functions
The project includes the following key functions for interacting with the Spotify dataset:

- query_create: Adds a new record to the DataFrame.
- query_read: Displays a specified number of records from the DataFrame.
- query_update: Updates the artist_name based on music_id.
- query_delete: Deletes a record from the DataFrame based on music_id.

Each function is designed to make CRUD operations simple and efficient on large datasets.

## Dataset Schema

The data used for this project came from [RunCHIRON](https://github.com/RunCHIRON/dataset) and contains a csv file of the top Spotify songs from 2023.

The Spotify dataset contains the following columns:

- `music_id`: Unique identifier for each track
- `track_name`: Name of the track
- `artist_name`: Artist's name
- `artist_count`: Count of artists
- `released_year`, `released_month`, released_day`: Release date information
- `in_spotify_playlists`, `in_spotify_charts`: Spotify-specific metrics
- `streams`: Total number of streams
- `in_apple_playlists`, `in_apple_charts`: Apple Music-specific metrics
- `in_deezer_playlists`, `in_deezer_charts`: Deezer-specific metrics
- `in_shazam_charts`: Shazam-specific metric
- Track Features: `bpm`, `key`, `mode`, `danceability_percent`, `valence_percent`, `energy_percent`, `acousticness_percent`, `instrumentalness_percent`, `liveness_percent`, `speechiness_percent`

## Example CRUD Operations
- Creating a Record
```python
query_create(music_id=1, track_name="New Song", artist_name="New Artist", ...)
```

- Reading Records
```python
query_read(limit=10)
```
- Updating a Record
```python
query_update(music_id=1, artist_name="Updated Artist")
```
- Deleting a Record
```python
query_delete(music_id=1)
```

## Analysis Queries in query.py

The query.py script includes additional analysis queries designed to provide deeper insights from the dataset. These queries include:

1. Retrieve Records by Release Year: Filters the dataset to show records from a specific release year, which can help analyze the music trends or song releases for that year.

```python
def query_read_by_year(self, year):
    result_df = self.spotify_df.filter(self.spotify_df.released_year == year)
    result_df.show()
    return "Read by Year Success"

```
2. Calculate Average Streams: Computes the average number of streams across all records, giving insights into the overall popularity and streaming trends.

```python
def query_average_streams(self):
    avg_streams = self.spotify_df.agg(avg("streams")).first()[0]
    return avg_streams if avg_streams is not None else 0

```
## Contributing
Contributions are welcome! Please fork the repository, make your changes, and submit a pull request.

## License
This project is licensed under the MIT License. See the LICENSE file for details.
