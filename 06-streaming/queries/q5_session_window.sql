SELECT PULocationID, num_trips
FROM session_window_counts
ORDER BY num_trips DESC
LIMIT 1;