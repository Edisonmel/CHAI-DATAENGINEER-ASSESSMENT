-- Create schema for staging tables
CREATE SCHEMA IF NOT EXISTS staging;

-- Creating Schema for analytics models
CREATE SCHEMA IF NOT EXISTS mart;

-- Artists
CREATE TABLE IF NOT EXISTS staging.stg_artists (
    artist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at DATE
);

CREATE INDEX IF NOT EXISTS idx_stg_artists_name
ON staging.stg_artists(name);

-- Albums
CREATE TABLE IF NOT EXISTS staging.stg_albums (
    album_id TEXT PRIMARY KEY,
    main_artist_id TEXT NOT NULL,
    name TEXT NOT NULL,
    release_date DATE,
    release_date_precision TEXT,
    total_tracks INTEGER CHECK (total_tracks >= 0),
    album_type TEXT NOT NULL,
    created_at DATE,

    CONSTRAINT fk_album_artist
        FOREIGN KEY (main_artist_id)
        REFERENCES staging.stg_artists(artist_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_stg_albums_release_date
ON staging.stg_albums(release_date);

-- Tracks
CREATE TABLE IF NOT EXISTS staging.stg_tracks (
    track_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    album_id TEXT NOT NULL,
    popularity INTEGER,
    duration_ms INTEGER CHECK (duration_ms >= 0),
    track_number INTEGER CHECK (track_number >= 1),
    disc_number INTEGER CHECK (disc_number >= 1),
    is_local BOOLEAN NOT NULL,
    created_at DATE,

    CONSTRAINT fk_track_artist
        FOREIGN KEY (artist_id)
        REFERENCES staging.stg_artists(artist_id),

    CONSTRAINT fk_track_album
        FOREIGN KEY (album_id)
        REFERENCES staging.stg_albums(album_id)
);
