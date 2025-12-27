INSERT INTO cron_settings (name, cron, is_enabled)
VALUES ('cleanup', '15 2 * * *', TRUE)
ON CONFLICT (name) DO NOTHING;
