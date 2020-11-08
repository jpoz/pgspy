SELECT
	tracks.id as id,
FROM tracks
JOIN songs ON songs.id = tracks.song_id
WHERE tracks.booking_id = 1
AND tracks.status <> 'completed'
ORDER BY tracks.sequence_id ASC
