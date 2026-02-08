import json
import logging
from datetime import datetime



def normalize_date(date_str: str):
    """
    Convert release_date string to Python date object.
    Handles full date, year-month, or year only.
    """
    if not date_str:
        return None

    try:
        # Full date: YYYY-MM-DD
        if len(date_str) == 10:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        # Year-Month: YYYY-MM
        elif len(date_str) == 7:
            return datetime.strptime(date_str, "%Y-%m").date().replace(day=1)
        # Year only: YYYY
        elif len(date_str) == 4:
            return datetime.strptime(date_str, "%Y").date().replace(month=1, day=1)
        else:
            # fallback
            return None
    except Exception as e:
        print(f"Error parsing release_date {date_str}: {e}")
        return None


def extract_upload_date_from_object_key(object_key: str):
    """
    Extracts the date part from a MinIO object key and converts it to a Python date object.
    
    Example:
        "/spotify_data/artists/artists_2026-02-07.json" -> datetime.date(2026, 2, 7)
    """
    try:
        # Extract the filename from object key
        filename = str(object_key).split("/")[-1]

        # Extract the date part (after last underscore and before .json)
        if "_" not in filename or not filename.endswith(".json"):
            raise ValueError(f"Object key filename '{filename}' is not in the expected format.")

        date_str = filename.split("_")[-1].replace(".json", "")

        # Convert to Python date object
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        return date_obj

    except (ValueError, TypeError) as e:
        logging.error(f"Failed to extract date from object key '{object_key}': {e}")
        raise
    


def validate_source_data(data: dict, object_key: str) -> bool:
    """
    Validate raw tracks JSON from MinIO before transformation.

    Checks:
        - Data is a dictionary and has "tracks" key
        - "tracks" is a non-empty list
        - Each track has required fields: id, name, album
        - Each album has required fields: id, name, release_date, album_type
        - Main artist info exists for album
    """
    logger = logging.getLogger("spotify_pipeline")

    # Check top-level structure
    if not isinstance(data, dict):
        logger.warning(f"Invalid data type for object '{object_key}': expected dict, got {type(data)}")
        return False

    tracks = data.get("tracks")
    if not tracks or not isinstance(tracks, list):
        logger.warning(f"No tracks list found in object '{object_key}' or tracks is empty.")
        return False

    # Validate each track
    for i, track in enumerate(tracks):
        if not isinstance(track, dict):
            logger.warning(f"Track {i} in '{object_key}' is not a dict")
            return False

        # Required track fields
        required_track_fields = ["id", "name", "album"]
        missing_fields = [f for f in required_track_fields if not track.get(f)]
        if missing_fields:
            logger.warning(f"Track {i} in '{object_key}' missing required fields: {missing_fields}")
            return False

        album = track.get("album", {})
        if not isinstance(album, dict):
            logger.warning(f"Track {i} album in '{object_key}' is not a dict")
            return False

        # Required album fields
        required_album_fields = ["id", "name", "release_date", "album_type", "artists"]
        missing_album_fields = [f for f in required_album_fields if not album.get(f)]
        if missing_album_fields:
            logger.warning(f"Track {i} album in '{object_key}' missing fields: {missing_album_fields}")
            return False

        # Check main artist exists
        album_artists = album.get("artists", [])
        if not album_artists or not album_artists[0].get("id") or not album_artists[0].get("name"):
            logger.warning(f"Track {i} album main artist missing in '{object_key}'")
            return False

    logger.info(f"All tracks in '{object_key}' passed validation")
    return True


