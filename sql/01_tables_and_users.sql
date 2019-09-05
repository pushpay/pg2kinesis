CREATE TABLE IF NOT EXISTS table_with_pk (
       id SERIAL NOT NULL PRIMARY KEY,
       itemname VARCHAR(40) NOT NULL,
       description VARCHAR(40)
);

CREATE TABLE IF NOT EXISTS table_without_pk (
       itemname VARCHAR(40) NOT NULL,
       description VARCHAR(40)
);

CREATE ROLE replicator WITH LOGIN PASSWORD 'replicator' REPLICATION;

GRANT CONNECT ON DATABASE pg2kinesis_test TO replicator;
