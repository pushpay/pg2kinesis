from collections import namedtuple
import os
import threading
import time

import psycopg2
import psycopg2.extras
import psycopg2.extensions
import psycopg2.errorcodes
import psycopg2.errors

from .log import logger

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, None)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, None)

PrimaryKeyMapItem = namedtuple('PrimaryKeyMapItem', 'table_name, col_name, col_type, col_ord_pos')

SLOT_IN_USE_RETRY_SECONDS = 30
SLOT_IN_USE_RETRY_LIMIT = 30

class SlotReader(object):
    PK_SQL = """
    SELECT CONCAT(table_schema, '.', table_name), column_name, data_type, ordinal_position
    FROM information_schema.tables
    LEFT JOIN (
        SELECT CONCAT(table_schema, '.', table_name), column_name, data_type, c.ordinal_position,
                    table_catalog, table_schema, table_name
        FROM information_schema.table_constraints
        JOIN information_schema.key_column_usage AS kcu
            USING (constraint_catalog, constraint_schema, constraint_name,
                        table_catalog, table_schema, table_name)
        JOIN information_schema.columns AS c
            USING (table_catalog, table_schema, table_name, column_name)
        WHERE constraint_type = 'PRIMARY KEY'
    ) as q using (table_catalog, table_schema, table_name)
    ORDER BY ordinal_position;
    """

    def __init__(self, database, host, port, user, sslmode, slot_name,
                 output_plugin='test_decoding', wal2json_write_in_chunks=False):
        # Cool fact: using connections as context manager doesn't close them on
        # success after leaving with block
        self._db_confg = dict(database=database, host=host, port=port, user=user, sslmode=sslmode)
        self._db_conn_str = os.getenv('PG2KINESIS_POSTGRES_CONNECTION')
        self._repl_conn = None
        self._repl_cursor = None
        self._normal_conn = None
        self.slot_name = slot_name
        self.output_plugin = output_plugin
        self.wal2json_write_in_chunks = wal2json_write_in_chunks
        self.cur_lag = 0

    def __enter__(self):
        self._normal_conn = self._get_connection()
        self._normal_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self._repl_conn = self._get_connection(connection_factory=psycopg2.extras.LogicalReplicationConnection)
        self._repl_cursor = self._repl_conn.cursor()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Be a good citizen and try to clean up on the way out.
        """

        try:
            self._repl_cursor.close()
        except Exception:
            pass

        try:
            self._repl_conn.close()
        except Exception:
            pass

        try:
            self._normal_conn.close()
        except Exception:
            pass

    def _get_connection(self, connection_factory=None, cursor_factory=None):
        if self._db_conn_str:
            # connection string environment variable takes precedence
            logger.info('Using PG2KINESIS_POSTGRES_CONNECTION connection string')
            return psycopg2.connect(self._db_conn_str,
                                    connection_factory=connection_factory,
                                    cursor_factory=cursor_factory)
        else:
            return psycopg2.connect(connection_factory=connection_factory,
                                    cursor_factory=cursor_factory, **self._db_confg)

    def _execute_and_fetch(self, sql, *params):
        with self._normal_conn.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)

            return cur.fetchall()

    @property
    def primary_key_map(self):
        logger.info('Getting primary key map')
        result = map(PrimaryKeyMapItem._make, self._execute_and_fetch(SlotReader.PK_SQL))
        pk_map = {rec.table_name: rec for rec in result}

        return pk_map

    def create_slot(self):
        logger.info('Creating slot %s' % self.slot_name)
        try:
            self._repl_cursor.create_replication_slot(self.slot_name,
                                                      slot_type=psycopg2.extras.REPLICATION_LOGICAL,
                                                      output_plugin=self.output_plugin)
        except psycopg2.ProgrammingError as p:
            # Will be raised if slot exists already.
            if p.pgcode != psycopg2.errorcodes.DUPLICATE_OBJECT:
                logger.error(p)
                raise
            else:
                logger.info('Slot %s is already present.' % self.slot_name)

    def delete_slot(self):
        logger.info('Deleting slot %s' % self.slot_name)
        try:
            self._repl_cursor.drop_replication_slot(self.slot_name)
        except psycopg2.ProgrammingError as p:
            # Will be raised if slot exists already.
            if p.pgcode != psycopg2.errorcodes.UNDEFINED_OBJECT:
                logger.error(p)
                raise
            else:
                logger.info('Slot %s was not found.' % self.slot_name)

    def process_replication_stream(self, consume):
        if self.output_plugin == 'wal2json':
            options = {'include-xids': 1, 'include-timestamp': 1}
            if self.wal2json_write_in_chunks:
                options['write-in-chunks'] = 1
        else:
            options = None
        logger.info('Output plugin options: "%s"' % options)
        retries = 0
        while True:
            try:
                self._repl_cursor.start_replication(self.slot_name, options=options)
            except psycopg2.errors.ObjectInUse as e:
                logger.warning(e)
                logger.info('Replication slot "%s" is in use. Sleeping for %s and trying again...' %
                            (self.slot_name, SLOT_IN_USE_RETRY_SECONDS))
                retries += 1
                time.sleep(SLOT_IN_USE_RETRY_SECONDS)
                if retries >= SLOT_IN_USE_RETRY_LIMIT:
                    logger.error('Retry limit exceeded')
                    raise
            else:
                logger.info('Replication started on slot "%s"' % self.slot_name)
                break
        logger.info('Starting the consumption of slot "%s"!' % self.slot_name)
        self._repl_cursor.consume_stream(consume)

