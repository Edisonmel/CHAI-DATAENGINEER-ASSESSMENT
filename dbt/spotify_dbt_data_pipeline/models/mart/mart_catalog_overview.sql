--- This model help to respond to some of analytics questions like
-- How big is Spotify’s catalog?, What is the average track length?

SELECT
    COUNT(DISTINCT a.artist_id) AS total_artists,
    COUNT(DISTINCT al.album_id) AS total_albums,
    COUNT(DISTINCT t.track_id) AS total_tracks,

    AVG(t.duration_ms) / 60000.0 AS avg_track_minutes,
    AVG(t.popularity) AS avg_track_popularity

FROM {{ source('spotify_staging', 'stg_artists') }} a
LEFT JOIN {{ source('spotify_staging', 'stg_albums') }} al
    ON a.artist_id = al.main_artist_id
LEFT JOIN {{ source('spotify_staging', 'stg_tracks') }} t
    ON a.artist_id = t.artist_id
