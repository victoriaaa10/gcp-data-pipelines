SELECT window_start, total_tip_amount
FROM hourly_tip_amounts
ORDER BY total_tip_amount DESC
LIMIT 1;