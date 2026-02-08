--- This model help to respond to some of analytics questions like
-- Which albums are most popular?, Which albums are biggest?, Do newer albums perform better?

SELECT
    al.album_id,
    al.name AS album_name,
    al.main_artist_id,
    a.name AS artist_name,
    al.release_date,
    al.album_type,

    COUNT(t.track_id) AS total_tracks,
    AVG(t.popularity) AS avg_track_popularity,
    MAX(t.popularity) AS max_track_popularity,
    SUM(t.duration_ms) / 60000.0 AS album_minutes

FROM {{ source('spotify_staging', 'stg_albums') }} al
LEFT JOIN {{ source('spotify_staging', 'stg_artists') }} a
    ON al.main_artist_id = a.artist_id
LEFT JOIN {{ source('spotify_staging', 'stg_tracks') }} t
    ON al.album_id = t.album_id

GROUP BY
    al.album_id,
    al.name,
    al.main_artist_id,
    a.name,
    al.release_date,
    al.album_type
