from __future__ import division
import time

import click
import psycopg2

from .slot import SlotReader
from .formatter import get_formatter
from .writer import get_writer
from .stream import StreamWriter
from .log import logger

@click.command()
@click.option('--pg-dbname', '-d', help='Database to connect to.')
@click.option('--pg-host', '-h', default='',
              help='Postgres server location. Leave empty if localhost.')
@click.option('--pg-port', '-p', default='5432', help='Postgres port.')
@click.option('--pg-user', '-u', help='Postgres user', default='postgres')
@click.option('--pg-sslmode', help='Postgres SSL mode', default='prefer')
@click.option('--pg-slot-name', '-s', default='pg2kinesis',
              help='Postgres replication slot name.')
@click.option('--pg-slot-output-plugin', default='test_decoding',
              type=click.Choice(['test_decoding', 'wal2json']),
              help='Postgres replication slot output plugin')
@click.option('--stream-name', '-k', default='pg2kinesis',
              help='Kinesis stream name.')
@click.option('--message-formatter', '-f', default='CSVPayload',
              type=click.Choice(['CSVPayload', 'CSV', 'JSONLine', 'ChunkJSONLine']),
              help='Kinesis record formatter.')
@click.option('--table-pat', help='Optional regular expression for table names.')
@click.option('--full-change', default=False, is_flag=True,
              help='Emit all columns of a changed row.')
@click.option('--create-slot', default=False, is_flag=True,
              help='Attempt to on start create a the slot.')
@click.option('--recreate-slot', default=False, is_flag=True,
              help='Deletes the slot on start if it exists and then creates.')
@click.option('--writer', '-w', default='stream',
              type=click.Choice(['stream', 'log', 'firehose']),
              help='Which writer to use')
@click.option('--send-window', '-t', default=15,
              type=click.INT,
              help='Number of seconds to wait before sending a non-full batch to the stream')
@click.option('--wal2json-write-in-chunks', default=False,
              is_flag=True,
              help='Enable write-in-chunks option for wal2json')
def main(pg_dbname, pg_host, pg_port, pg_user, pg_sslmode, pg_slot_name, pg_slot_output_plugin,
         stream_name, message_formatter, table_pat, full_change, create_slot, recreate_slot,
         writer, send_window, wal2json_write_in_chunks):

    if full_change:
        assert message_formatter in ['CSVPayload', 'JSONLine'], 'Full changes must be formatted as JSON.'
        assert pg_slot_output_plugin == 'wal2json', 'Full changes must use wal2json.'

    if wal2json_write_in_chunks:
        logger.info('write-in-chunks enabled, ignoring formatter option and using ChunkJSONLineFormatter')
        message_formatter = 'ChunkJSONLine'

    logger.info('Starting pg2kinesis')
    if writer == 'stream':
        logger.info('Getting kinesis stream writer')
        writer = StreamWriter(stream_name)
    else:
        logger.info('Getting %s writer', writer)
        writer = get_writer(writer, stream_name, send_window=send_window)

    while True:
        try:
            with SlotReader(pg_dbname, pg_host, pg_port, pg_user, pg_sslmode, pg_slot_name,
                            output_plugin=pg_slot_output_plugin,
                            wal2json_write_in_chunks=wal2json_write_in_chunks) as reader:

                if recreate_slot:
                    reader.delete_slot()
                    reader.create_slot()
                elif create_slot:
                    reader.create_slot()

                pk_map = reader.primary_key_map
                formatter = get_formatter(message_formatter, pk_map,
                                        pg_slot_output_plugin, full_change, table_pat)

                consume = Consume(formatter, writer)

                # Blocking. Responds to Control-C.
                reader.process_replication_stream(consume)
        except psycopg2.OperationalError as e:
            if e.pgerror and 'server closed the connection unexpectedly' in e.pgerror:
                # this is a workaround for Aurora frequently closing the connection
                # start a new connection and continue replication
                logger.warning(e)
                logger.info('Restarting the connection...')
            else:
                raise

class Consume(object):
    def __init__(self, formatter, writer):
        self.cum_msg_count = 0
        self.cum_msg_size = 0
        self.msg_window_size = 0
        self.msg_window_count = 0
        self.cur_window = 0

        self.formatter = formatter
        self.writer = writer

    def __call__(self, change):
        self.cum_msg_count += 1
        self.cum_msg_size += change.data_size

        self.msg_window_size += change.data_size
        self.msg_window_count += 1

        logger.debug('ReplicationMessage: data_size=%s data_start=%s wal_end=%s send_time=%s', change.data_size, change.data_start, change.wal_end, change.send_time)
        fmt_msgs = self.formatter(change.payload)
        logger.debug('Got %s change messages', len(fmt_msgs))

        progress_msg = 'xid: {:12} win_count:{:>10} win_size:{:>10}mb cum_count:{:>10} cum_size:{:>10}mb'

        for fmt_msg in fmt_msgs:
            did_put = self.writer.put_message(fmt_msg)
            if did_put:
                change.cursor.send_feedback(flush_lsn=change.data_start)
                logger.info('Flushed LSN: {}'.format(change.data_start))

            int_time = int(time.time())
            if not int_time % 10 and int_time != self.cur_window:
                logger.info(progress_msg.format(
                    self.formatter.cur_xact, self.msg_window_count,
                    self.msg_window_size / 1048576, self.cum_msg_count,
                    self.cum_msg_size / 1048576))

                self.cur_window = int_time
                self.msg_window_size = 0
                self.msg_window_count = 0

if __name__ == '__main__':
    main()
