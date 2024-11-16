"""
Test Databricks Functionality
"""

import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/tmp/Spotify_2023.csv"  # Update this path as per your setup
BASE_URL = f"https://{SERVER_HOSTNAME}/api/2.0"

# Validate environment variables
if not SERVER_HOSTNAME or not ACCESS_TOKEN:
    raise ValueError("SERVER_HOSTNAME and ACCESS_TOKEN must be set in the .env file.")


def check_filestore_path(path: str, headers: dict) -> bool:
    """
    Check if a file path exists in Databricks DBFS and validate authentication.

    Args:
        path (str): The file path to check.
        headers (dict): Headers including the authorization token.

    Returns:
        bool: True if the file path exists and authentication is valid, False otherwise.
    """
    try:
        response = requests.get(
            f"{BASE_URL}/dbfs/get-status", headers=headers, params={"path": path}
        )
        response.raise_for_status()
        # Check if the response contains a valid file path
        return "path" in response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    return False


def test_databricks():
    """
    Test if the Databricks file store path exists and is accessible.
    """
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    path_exists = check_filestore_path(FILESTORE_PATH, headers)
    assert (
        path_exists
    ), f"File path '{FILESTORE_PATH}' does not exist or authentication failed."
    print(
        f"Test successful: The file path '{FILESTORE_PATH}' exists and is accessible."
    )


if __name__ == "__main__":
    test_databricks()
