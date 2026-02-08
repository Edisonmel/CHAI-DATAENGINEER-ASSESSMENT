--- This model help to respond to some of analytics questions like
-- What are the top tracks?, Are explicit tracks more popular?, How long are popular tracks?

SELECT
    t.track_id,
    t.name AS track_name,
    t.album_id,
    al.name AS album_name,
    t.artist_id,
    a.name AS artist_name,

    t.popularity,
    t.duration_ms / 1000.0 AS duration_seconds,

    CASE
        WHEN t.popularity >= 80 THEN 'hit'
        WHEN t.popularity >= 50 THEN 'medium'
        ELSE 'low'
    END AS popularity_bucket

FROM {{ source('spotify_staging', 'stg_tracks') }} t
LEFT JOIN {{ source('spotify_staging', 'stg_albums') }} al
    ON t.album_id = al.album_id
LEFT JOIN {{ source('spotify_staging', 'stg_artists') }} a
    ON t.artist_id = a.artist_id
