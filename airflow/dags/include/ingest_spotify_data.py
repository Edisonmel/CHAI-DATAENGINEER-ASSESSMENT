import os
import json
import logging
from datetime import datetime
import io
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from botocore.exceptions import BotoCoreError, ClientError
import sys
import json
import requests
from airflow.models import Variable



logger = logging.getLogger("spotify_pipeline")


def read_spotify_ids(json_file_path: str) -> tuple[list[str], list[str]]:
    """
    Reads a JSON file with artist_ids and tracker_ids and returns two lists.
    """
    try:

        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # artist_ids = data['sportify_ids']['artist_ids']
        tracker_ids = data['sportify_ids']['tracker_ids']
    
        return tracker_ids
    except Exception as e:
        logging.error(f"Error occured while reading stotify ids json file: {e}")
        raise Exception("Error occured while reading stotify ids json file")
    
    
    except Exception as e:
        logging.error(f"Error Occured While trying to fetch data for artists: {e}")
        raise Exception("Error Occured While trying to fetch data for artists")


def fetch_tracks_data(base_url: str, tracker_ids: list[str], bearer_token: str) -> dict:
    """
    Fetch track details from Spotify API given a list of track IDs.
    """

    if not tracker_ids:
        raise ValueError("artist_ids list cannot be empty.")
    
    if not bearer_token:
        raise ValueError("Bearer token is required for authentication.")
    
    ids_str = ",".join(tracker_ids)
    url = f"{base_url}/tracks?ids={ids_str}"
    headers = {'Authorization': f'Bearer {bearer_token}'}

    try:
        logging.info(f"Start Pulling Tracks data at {url}")
        response = requests.get(url, headers=headers, timeout=10)

        # Fail the task if status code is not 200
        if response.status_code != 200:
            logging.error(
                f"Failed to fetch tracks data. Status code: {response.status_code}, Response: {response.text}"
            )
            raise Exception(f"Spotify API request  for pulling tracks data failed with status code {response.status_code}")
        return response.json()
    
    except Exception as e:
        logging.error(f"Error Occured While trying to fetch data for artists: {e}")
        raise Exception("Error Occured While trying to fetch data for artists")
    

# Module 2: Create Bucket

def create_bucket_if_not_exists(s3_client, bucket_name: str):
    """
    Ensure MinIO bucket exists, create if missing.
    """
    try:
        existing_buckets = [b['Name'] for b in s3_client.list_buckets().get('Buckets', [])]
        if bucket_name not in existing_buckets:
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"Created MinIO bucket: {bucket_name}")
        else:
            logger.info(f"MinIO bucket '{bucket_name}' already exists.")
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Failed to create or check bucket: {e}")
        raise

# Module 3: Upload JSON Directly to MinIO

def upload_json_to_minio(
    data: dict,
    bucket_name: str,
    data_category: str = "tracks",
    minio_folder: str = "spotify_data",
    minio_endpoint: str = "minio:9000",
    minio_access_key: str = None,
    minio_secret_key: str = None,
    secure: bool = False,
    pretty: bool = True
) -> str:
    """
    Upload artists or trackers JSON to MinIO with detailed logging.
    """
    try:
        logger.info("Starting MinIO upload process...")

        # Step 1: Credentials
        minio_access_key = minio_access_key or os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        minio_secret_key = minio_secret_key or os.environ.get("MINIO_SECRET_KEY", "minioadmin123")

        if not minio_access_key or not minio_secret_key:
            raise ValueError("MinIO credentials not provided")

        # Step 2: Create S3 client
        s3_client = boto3.client(
            "s3",
            endpoint_url=f"http{'s' if secure else ''}://{minio_endpoint}",
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
        )


        logger.info(f"S3 client created for endpoint '{minio_endpoint}' (secure={secure}).")

        # Step 3: Ensure bucket exists
        create_bucket_if_not_exists(s3_client, bucket_name)
        logger.info(f"Bucket '{bucket_name}' verified/created.")

        # Prepare object key
        timestamp = datetime.now().strftime("%Y-%m-%d")
        object_key = f"{minio_folder}/{data_category}/{data_category}_{timestamp}.json"
        

        # Convert dict to JSON bytes buffer
        json_bytes = json.dumps(data, indent=4 if pretty else None).encode("utf-8")
        buffer = io.BytesIO(json_bytes)
        
        # Step 6: Upload to MinIO
        s3_client.upload_fileobj(buffer, bucket_name, object_key)
        logger.info(f"Uploaded '{data_category}' JSON to MinIO bucket '{bucket_name}' at '{object_key}'.")
        return object_key

    except (BotoCoreError, ClientError) as e:
        logger.error(f"MinIO upload error: {e}")
        return ""
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return ""
    

    

# if __name__ == "__main__":
#     main()

