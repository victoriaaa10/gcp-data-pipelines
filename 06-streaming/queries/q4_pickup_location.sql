SELECT PULocationID, num_trips
FROM pickup_location_counts
ORDER BY num_trips DESC
LIMIT 3;