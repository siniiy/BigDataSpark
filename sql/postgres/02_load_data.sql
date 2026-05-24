-- Load all 10 CSV files into the raw mock_data table.
-- Files are mounted at /data inside the postgres container.
COPY mock_data FROM '/data/MOCK_DATA.csv'    CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (1).csv' CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (2).csv' CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (3).csv' CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (4).csv' CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (5).csv' CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (6).csv' CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (7).csv' CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (8).csv' CSV HEADER;
COPY mock_data FROM '/data/MOCK_DATA (9).csv' CSV HEADER;
