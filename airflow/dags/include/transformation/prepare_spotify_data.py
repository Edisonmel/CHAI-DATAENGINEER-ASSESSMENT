import json
import logging
from datetime import datetime
from include.helpers import normalize_date,extract_upload_date_from_object_key,validate_source_data



# Deffine function to transforming

def transform_tracks_data(data: dict,object_key):
    """Transform tracks JSON into normalized tables (lists of objects)."""

    # Run validation test to ensure the source data is meeting the expectation
    validation_result = validate_source_data(data,object_key)

    if not validation_result:
        raise Exception("Can't processed with transformation since the data is not matching the expectation")

    artists_map = {}
    albums_map = {}
    tracks_list = []

    for track in data.get("tracks", []):

        # Track info
        track_id = track.get("id")
        if not track_id:
            continue

        track_name = track.get("name")
        popularity = track.get("popularity")
        duration_ms = track.get("duration_ms")
        track_number = track.get("track_number")
        disc_number = track.get("disc_number")
        is_local = track.get("is_local", False)

        # Album info
        album = track.get("album", {})
        album_id = album.get("id")
        album_name = album.get("name")
        release_date = normalize_date(str(album.get("release_date")))
        release_precision = album.get("release_date_precision")
        total_tracks = album.get("total_tracks")
        album_type = album.get("album_type")

        
        # Album main artist
        album_artists = album.get("artists", [])
        if album_artists:
            main_artist = album_artists[0]
            artist_id = main_artist.get("id")
            artist_name = main_artist.get("name")
        else:
            artist_id = None
            artist_name = None

        # Save artist (deduplicated)
        if artist_id and artist_id not in artists_map:
            artists_map[artist_id] = {
                "artist_id": artist_id,
                "name": artist_name,
                "created_at": extract_upload_date_from_object_key(object_key)
            }

        # Save album (deduplicated)
        if album_id and album_id not in albums_map:
            albums_map[album_id] = {
                "album_id": album_id,
                "main_artist_id": artist_id,
                "name": album_name,
                "release_date": normalize_date(str(release_date)),
                "release_date_precision": release_precision,
                "total_tracks": total_tracks,
                "album_type": album_type,
                "created_at": extract_upload_date_from_object_key(object_key)
            }

        # Save track (list) 
        tracks_list.append({
            "track_id": track_id,
            "name": track_name,
            "artist_id": artist_id,   
            "album_id": album_id,    
            "popularity": popularity,
            "duration_ms": duration_ms,
            "track_number": track_number,
            "disc_number": disc_number,
            "is_local": is_local,
            "created_at": extract_upload_date_from_object_key(object_key)
        })

    # Convert maps → lists
    artists_list = list(artists_map.values())
    albums_list = list(albums_map.values())

    print(tracks_list[0])
    print(albums_list[0])

    return artists_list,albums_list, tracks_list