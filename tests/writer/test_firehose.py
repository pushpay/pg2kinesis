import time

from freezegun import freeze_time
from mock import patch, call, Mock
import pytest
import boto3
from botocore.exceptions import ClientError

from pg2kinesis.writer.firehose import FirehoseWriter

@pytest.fixture()
def writer():
    with patch.object(boto3, 'client'):
        writer = FirehoseWriter('blah')
    return writer

def test__init__():
    mock_client = Mock()
    with patch.object(boto3, 'client', return_value=mock_client):
        error_response = {'Error': {'Code': 'ResourceInUseException'}}

        FirehoseWriter('blah')
        assert call.describe_delivery_stream(DeliveryStreamName='blah') in mock_client.method_calls, "We checked firehose existence"


def test_put_message(writer):

    writer._send_agg_record = Mock()

    msg = Mock()
    msg.change.xid = 10
    msg.fmt_msg = object()

    writer.last_send = 1445444940.0 - 10      # "2015-10-21 16:28:50"
    with freeze_time('2015-10-21 16:29:00'):  # -> 1445444940.0
        result = writer.put_message(None)

        assert result is None, 'With no message or timeout we did not force a send'
        assert not writer._send_agg_record.called, 'we did not force a send'

        writer._record_agg.add_user_record = Mock(return_value=None)
        result = writer.put_message(msg)
        assert result is None, 'With message, no timeout and not a full agg we do not send'
        assert not writer._send_agg_record.called, 'we did not force a send'

    with freeze_time('2015-10-21 16:29:10'):  # -> 1445444950.0
        result = writer.put_message(None)
        assert result is not None, 'Timeout forced a send'
        assert writer._send_agg_record.called, 'We sent a record'
        assert writer.last_send == 1445444950.0, 'updated window'

    with freeze_time('2015-10-21 16:29:20'):  # -> 1445444960.0
        writer._send_agg_record.reset_mock()
        writer._record_agg.add_user_record = Mock(return_value='blue')
        result = writer.put_message(msg)

        assert result == 'blue', 'We passed in a message that forced the agg to report full'
        assert writer._send_agg_record.called, 'We sent a record'
        assert writer.last_send == 1445444960.0, 'updated window'


def test__send_agg_record_empty(writer):
    assert writer._send_agg_record(None) is None, 'Do not do anything if agg_record is None'


def test__send_agg_record_service_unavailable(writer):

    agg_rec = Mock()
    agg_rec.get_contents = Mock(return_value=[{'Data': 'blob'}])

    err = ClientError({'Error': {'Code': 'ServiceUnavailableException'}}, 'put_record_batch')

    writer._firehose.put_record_batch = Mock(
        side_effect=[
            err, err, err,
            {'FailedPutCount': 0,
             'Encrypted': False,
             'RequestResponses': [{'RecordId': '1', 'ErrorCode': None, 'ErrorMessage': None}]}
        ]
    )

    with patch.object(time, 'sleep') as mock_sleep:
        writer._send_agg_record(agg_rec)
        assert mock_sleep.call_count == 3, "We had to back off 3 times so we slept"
        assert mock_sleep.call_args_list == [call(.1), call(.2), call(.4)], 'Geometric back off!'

def test__send_agg_record_client_error(writer):
    agg_rec = Mock()
    agg_rec.get_contents = Mock(return_value=[{'Data': 'blob'}])

    with pytest.raises(ClientError):
        writer._firehose.put_record_batch = Mock(side_effect=ClientError({'Error': {'Code': 'Something else'}},
                                                                  'put_record_batch'))
        writer._send_agg_record(agg_rec)

def test__send_agg_record_too_many_backoff(writer):
    agg_rec = Mock()
    agg_rec.get_contents = Mock(return_value=[{'Data': 'blob'}])

    err = ClientError({'Error': {'Code': 'ServiceUnavailableException'}}, 'put_record_batch')

    writer.back_off_limit = .3  # Will bust on third go around
    writer._firehose.put_record_batch = Mock(
        side_effect=[
            err, err, err,
            {'FailedPutCount': 0,
             'Encrypted': False,
             'RequestResponses': [{'RecordId': '1', 'ErrorCode': None, 'ErrorMessage': None}]}
        ]
    )
    with pytest.raises(Exception) as e_info, patch.object(time, 'sleep'):
        writer._send_agg_record(agg_rec)
        assert e_info.value.message == 'ServiceUnavailableException caused a backed off too many times!', \
            'We raise on too many throughput errors'


def test__send_agg_record_failed_put_count(writer):
    agg_rec = Mock()
    agg_rec.get_contents = Mock(return_value=[{'Data': 'blob'}, {'Data': 'otherblob'}, {'Data': 'blah'}])

    writer._firehose.put_record_batch = Mock(
        side_effect=[
            {
                'FailedPutCount': 2,
                'Encrypted': False,
                'RequestResponses': [
                    {'RecordId': '1', 'ErrorCode': None, 'ErrorMessage': None},
                    {'RecordId': None, 'ErrorCode': 'Blah', 'ErrorMessage': 'Blah'},
                    {'RecordId': None, 'ErrorCode': 'Blah', 'ErrorMessage': 'Blah'},
                ]
            },
            {
                'FailedPutCount': 1,
                'Encrypted': False,
                'RequestResponses': [
                    {'RecordId': '2', 'ErrorCode': None, 'ErrorMessage': None},
                    {'RecordId': None, 'ErrorCode': 'Blah', 'ErrorMessage': 'Blah'},
                ]
            },
            {
                'FailedPutCount': 0,
                'Encrypted': False,
                'RequestResponses': [
                    {'RecordId': '3', 'ErrorCode': None, 'ErrorMessage': None},
                ]
            },
        ]
    )
    writer._send_agg_record(agg_rec)
    assert writer._firehose.put_record_batch.call_count == 3, 'PutRecordBatch was retried twice'

    # the last call to PutBatchRecord is the last item that failed
    writer._firehose.put_record_batch.assert_called_with(DeliveryStreamName='blah', Records=[{'Data': b'blah'}])



def test__reaggregate_records():
    pass


def test_agg_record():
    pass


def test_firehose_record_aggregator():
    pass
