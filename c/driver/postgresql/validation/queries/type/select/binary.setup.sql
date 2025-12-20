CREATE TABLE test_binary (
    idx INTEGER,
    res BYTEA
);

INSERT INTO test_binary (idx, res) VALUES (1, E'\\xe38193e38293e381abe381a1e381afe38081e4b896e7958cefbc81');  -- 'こんにちは、世界！' in UTF-8
INSERT INTO test_binary (idx, res) VALUES (2, E'\\x00');
INSERT INTO test_binary (idx, res) VALUES (3, E'\\xdeadbeef');
INSERT INTO test_binary (idx, res) VALUES (4, E'\\x');
INSERT INTO test_binary (idx, res) VALUES (5, NULL);
