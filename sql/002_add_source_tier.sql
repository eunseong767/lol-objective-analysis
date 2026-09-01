ALTER TABLE match_queue ADD COLUMN source_tier TEXT;
UPDATE match_queue SET source_tier = 'CHALLENGER_GM' WHERE source_tier IS NULL;