import os
import requests
from azure.storage.blob import BlobClient
import time
import json
import io

# Azure connection details
conn_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
if not conn_string:
    raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")

container_name = "rawdatagdansk"

while True:
    try:
        # Fetch JSON data from the API
        response = requests.get("https://ckan2.multimediagdansk.pl/gpsPositions?v=2")
        response.raise_for_status()  # Raise exception for HTTP status codes >= 400

        # Parse JSON data
        json_data = response.json()

        # Ensure 'lastUpdate' is present, otherwise skip processing
        last_update = json_data.get('lastUpdate')
        if not last_update:
            print("Warning: 'lastUpdate' is missing from the API response. Skipping this iteration.")
            time.sleep(20)  # Wait before retrying
            continue

        # Generate a valid blob name using 'lastUpdate'
        blob_name = f"ZTM_data_{last_update.replace(':', '-').replace('T', '_').replace('Z', '')}.json"

    except requests.exceptions.RequestException as e:
        # Handle network and response-related errors
        print(f"Error fetching data from API: {e}")
        time.sleep(20)  # Wait before retrying
        continue

    except json.JSONDecodeError as e:
        # Handle JSON parsing issues
        print(f"Invalid JSON response: {e}")
        time.sleep(20)  # Wait before retrying
        continue

    except Exception as e:
        # Catch-all for unexpected errors in the fetch phase
        print(f"Unexpected error during data fetching: {e}")
        time.sleep(20)  # Wait before retrying
        continue

    try:
        # Prepare the BlobClient
        blob_client = BlobClient.from_connection_string(
            conn_string,
            container_name=container_name,
            blob_name=blob_name,
        )

        # Serialize JSON data to a string buffer (in-memory)
        json_string = json.dumps(json_data, indent=2)
        json_buffer = io.StringIO(json_string)

        # Step 3: Upload the blob to Azure Blob Storage
        blob_client.upload_blob(json_buffer.getvalue(), overwrite=False)
        print(f"Blob '{blob_name}' successfully uploaded to container '{container_name}'.")

    except Exception as e:
        # Handle Azure-related errors (e.g., already exists, connection issues)
        if "exists" in str(e):
            print(f"Blob '{blob_name}' already exists. Skipping upload.")
        else:
            print(f"Error uploading blob to Azure: {e}")

    # Wait before the next iteration
    time.sleep(20)