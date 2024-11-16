# """
#     Test Main
# """

# from main import main

# def test_function():
#     """
#     Executes the main function and captures its results.
#     """
#     # Run the main function and capture results
#     return main()


# if __name__ == "__main__":
#     # Run the test function and capture results
#     results = test_function()

#     # Debugging: Print results for manual inspection
#     print("Test Results:")
#     if results is None:
#         raise ValueError("Results dictionary is None. Ensure main() returns a valid results dictionary.")

#     for key, value in results.items():
#         print(f"{key}: {value}")

#     # Assertions to verify each operation's output
#     assert results.get("extract_to") == "dbfs:/tmp/Spotify_2023.csv", (
#         f"Extraction path mismatch: expected 'dbfs:/tmp/Spotify_2023.csv', got '{results.get('extract_to')}'"
#     )

#     assert results.get("transform_db") == "dbfs:/tmp/Spotify_Transformed.parquet", (
#         f"Transformation output path mismatch: expected 'dbfs:/tmp/Spotify_Transformed.parquet', got '{results.get('transform_db')}'"
#     )

#     assert results.get("create") == "Create Success", "Create operation failed"
#     assert results.get("read") == "Read Success", "Read operation failed"
#     assert results.get("update") == "Update Success", "Update operation failed"
#     assert results.get("delete") == "Delete Success", "Delete operation failed"

#     # Validate 'Read by Year' operation
#     read_by_year = results.get("read_by_year")
#     assert read_by_year and "Read by Year Success" in read_by_year, (
#         f"Read by Year operation failed: {read_by_year}"
#     )

#     # Validate average streams result
#     average_streams = results.get("average_streams")
#     assert isinstance(average_streams, (int, float)), (
#         f"Average streams result should be a number, got {type(average_streams)}"
#     )
#     assert average_streams >= 0, "Average streams should be 0 or greater"

#     # All tests passed
#     print("\nAll tests passed successfully.")

# """
"""
Test Databricks Functionality
"""
import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
server_hostname = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "/dbfs/tmp/Spotify_2023.csv"  # Correct file path based on extract.py
base_url = f"https://{server_hostname}/api/2.0"

# Function to check if a file path exists and authentication settings are valid
def check_filestore_path(path, headers):
    """
    Checks if a file path exists in Databricks DBFS and validates authentication.

    Args:
        path (str): The file path to check.
        headers (dict): Headers including the authorization token.

    Returns:
        bool: True if the file path exists and authentication is valid, False otherwise.
    """
    try:
        response = requests.get(base_url + f"/dbfs/get-status", headers=headers, params={"path": path})
        response.raise_for_status()
        # The path exists if the response contains 'path'
        return 'path' in response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error checking file path: {e}")
        return False

# Test if the specified FILESTORE_PATH exists
def test_databricks():
    """
    Tests if the Databricks file store path exists and is accessible.
    """
    headers = {'Authorization': f'Bearer {access_token}'}
    path_exists = check_filestore_path(FILESTORE_PATH, headers)
    assert path_exists, f"File path {FILESTORE_PATH} does not exist or authentication failed."

if __name__ == "__main__":
    test_databricks()
    print(f"Test successful: The file path '{FILESTORE_PATH}' exists and is accessible.")
