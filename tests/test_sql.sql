CREATE TABLE IF NOT EXISTS test_table (test_col VARCHAR);
INSERT INTO test_table VALUES ('bike');
DROP TABLE IF EXISTS test_table CASCADE;