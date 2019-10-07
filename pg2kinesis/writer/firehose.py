import time
import base64
import boto3

from botocore.exceptions import ClientError
from pg2kinesis.log import logger

MAX_BATCH_COUNT = 500
MAX_BATCH_BYTES = 1024*1024*4  # 4MB
MAX_RECORD_BYTES = 1024*1000 # 1000KB


class FirehoseRecordAggregator(object):
    """
    Records aggregator inspired by aws_kinesis_agg.aggregator

    NOTE: This object is not thread-safe.
    """

    def __init__(self):
        self.current_record = AggRecord()

    def add_user_record(self, data):
        success = self.current_record.add_user_record(data)

        if success:
            return

        out_record = self.current_record
        self.clear_record()
        self.current_record.add_user_record(data)
        return out_record

    def get_contents(self):
        return self.records

    def get_num_user_records(self):
        return self.current_record.get_num_user_records()

    def get_size_bytes(self):
        return self.current_record.get_size_bytes()

    def clear_record(self):
        self.current_record = AggRecord()

    def clear_and_get(self):
        out_record = self.current_record
        self.clear_record()
        return out_record


class AggRecord(object):
    """
    Represents aggregated Firehose records. Inspired by aws_kinesis_agg.aggregator.

    NOTE: This object is not thread-safe.
    """
    def __init__(self):
        self.current_count = 0
        self.current_bytes = 0
        self.records = []

    def add_user_record(self, data, b64encode=False):
        if isinstance(data, str):
            data = data.encode('utf8')

        if len(data) > MAX_RECORD_BYTES:
            # Each record in the request can be as large as 1,000 KB (before 64-bit encoding)
            raise ValueError('data must be less than %s bytes', MAX_RECORD_BYTES)

        if self.current_count >= MAX_BATCH_COUNT:
            # Each PutRecordBatch request supports up to 500 records.
            return False

        if b64encode:
            data = base64.b64encode(data)
        data_bytes = len(data)

        if data_bytes + self.current_bytes > MAX_BATCH_BYTES:
            # Each PutRecordBatch request supports up to 4MB for the entire request.
            return False

        self.records.append({'Data': data})
        self.current_count += 1
        self.current_bytes += data_bytes
        return True

    def get_contents(self):
        return self.records

    def get_num_user_records(self):
        return self.current_count

    def get_size_bytes(self):
        return self.current_bytes

    def clear_and_get(self):
        records = self.records
        self.records = []
        self.current_count = 0
        self.current_bytes = 0
        return records


class FirehoseWriter(object):
    def __init__(self, firehose_name, back_off_limit=60, send_window=15):
        self.firehose_name = firehose_name
        self.back_off_limit = back_off_limit
        self.last_send = 0

        self._firehose = boto3.client('firehose')
        self._record_agg = FirehoseRecordAggregator()
        self._send_window = send_window

        # check firehose stream is available
        self._firehose.describe_delivery_stream(DeliveryStreamName=self.firehose_name)

    def put_message(self, fmt_msg):
        agg_record = None

        if fmt_msg:
            agg_record = self._record_agg.add_user_record(fmt_msg.fmt_msg)

        # agg_record will be a complete record if aggregation is full.
        send_window_timeout = self._send_window and time.time() - self.last_send > self._send_window
        if agg_record or send_window_timeout:
            agg_record = agg_record if agg_record else self._record_agg.clear_and_get()
            self._send_agg_record(agg_record)
            self.last_send = time.time()

        return agg_record

    def _send_agg_record(self, agg_record):
        if agg_record is None:
            return

        back_off = 0.05
        failed_put_count = 0
        while back_off < self.back_off_limit:
            try:
                records = agg_record.get_contents()
                logger.info('Sending %s records. Size %s.',
                            agg_record.get_num_user_records(), agg_record.get_size_bytes())
                result = self._firehose.put_record_batch(Records=records,
                                                         DeliveryStreamName=self.firehose_name)
            except ClientError as e:
                if e.response['Error']['Code'] == 'ServiceUnavailableException':
                    back_off *= 2
                    logger.warning('Firehose throughput exceeded: sleeping %ss', back_off)
                    time.sleep(back_off)
                else:
                    logger.error(e)
                    raise
            else:
                failed_put_count = result['FailedPutCount']
                if failed_put_count > 0:
                    # retry the put with the successful records excluded
                    logger.warning('FailedPutCount: %s. Re-aggregating and retrying...', failed_put_count)
                    agg_record = self._reaggregate_records(records, result['RequestResponses'])
                    back_off *= 2
                    logger.warning('Firehose throughput exceeded: sleeping %ss', back_off)
                    time.sleep(back_off)
                else:
                    break
        else:
            raise Exception('ServiceUnavailableException caused a backed off too many times!')

    def _reaggregate_records(self, original_records, responses):
        """
        Re-aggregate failed records for resubmission.

        [RequestResponses] directly correlates with a record in the
        request array using the same ordering, from the top to the
        bottom. The response array always includes the same number of
        records as the request array.
        """
        new_agg_record = AggRecord()
        for i, response in enumerate(responses):
            if response.get('ErrorCode'):
                logger.warning('ErrorCode: "%s", ErrorMessage: "%s"', response['ErrorCode'], response['ErrorMessage'])
                new_agg_record.add_user_record(original_records[i]['Data'])
        return new_agg_record

