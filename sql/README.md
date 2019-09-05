# Local testing


Run the Debezium Postgres image. We use this image because it comes
with wal2json and logical replication pre-configured.
```
$ docker run -p 5432:5432 debezium/postgres:10-alpine
```

Create the database and the tables.
```
$ psql -h localhost -U postgres -f 00_database.sql -d postgres
$ psql -h localhost -U postgres -f 01_tables_and_users.sql -d postgres
```

Start `pg2kinesis` using the `log` writer.
```
$ PGPASSWORD=replicator pg2kinesis --writer=log --pg-dbname=pg2kinesis_test --pg-host=localhost --pg-user=replicator --create-slot --pg-slot-output-plugin=wal2json --full-change
```

Run the rest of the SQL scripts to see the replication logs.
```
$ psql -h localhost -U postgres -f 02_insert.sql -d pg2kinesis_test
$ psql -h localhost -U postgres -f 03_update.sql -d pg2kinesis_test
$ psql -h localhost -U postgres -f 04_delete.sql -d pg2kinesis_test
```
