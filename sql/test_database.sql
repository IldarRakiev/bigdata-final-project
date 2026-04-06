SELECT 'repositories' AS table_name, COUNT(*) AS row_count FROM repositories;
SELECT 'events' AS table_name, COUNT(*) AS row_count FROM events;
SELECT event_type, COUNT(*) AS rows, SUM(event_count) AS total_events FROM events GROUP BY event_type ORDER BY total_events DESC;
SELECT language, COUNT(*) AS repo_count FROM repositories WHERE language IS NOT NULL GROUP BY language ORDER BY repo_count DESC LIMIT 10;
