-- This model help to respond to some of analytics questions like
-- Who are the top artists?, Who has the biggest catalog?, Who has the most popular tracks?
SELECT
    a.artist_id,
    a.name AS artist_name,

    COUNT(DISTINCT al.album_id) AS total_albums,
    COUNT(DISTINCT t.track_id) AS total_tracks,

    AVG(t.popularity) AS avg_track_popularity,
    MAX(t.popularity) AS max_track_popularity,

    SUM(t.duration_ms) / 60000.0 AS total_minutes_of_music

FROM {{ source('spotify_staging', 'stg_artists') }} a
LEFT JOIN {{ source('spotify_staging', 'stg_albums') }} al
    ON a.artist_id = al.main_artist_id
LEFT JOIN {{ source('spotify_staging', 'stg_tracks') }} t
    ON a.artist_id = t.artist_id

GROUP BY a.artist_id, a.name
