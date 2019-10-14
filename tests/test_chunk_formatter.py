# coding=utf-8
from __future__ import unicode_literals
import json

import mock
import pytest

from pg2kinesis.slot import PrimaryKeyMapItem
from pg2kinesis.formatter import Change, ChunkJSONLineFormatter

@pytest.fixture
def pkey_map():
    return {'public.test_table': PrimaryKeyMapItem(u'public.test_table', u'uuid', u'uuid', 0),
            'public.test_table2': PrimaryKeyMapItem(u'public.test_table2', u'name', u'character varying', 0)}

@pytest.fixture(params=[ChunkJSONLineFormatter])
def formatter(request, pkey_map):
    return request.param(pkey_map)


def test__preprocess_wal2json_full_change(formatter):
    formatter.cur_xact = ''
    formatter.cur_timestamp = ''
    formatter.full_change = True

    # 1st chunk, setting metadata
    result = formatter._preprocess_wal2json_change(b"""{"xid": 101,
                "timestamp": "2019-09-04 01:27:59.195339+00",
                "change": [""")
    assert result == []
    assert formatter.cur_xact == 101
    assert formatter.cur_timestamp == '2019-09-04 01:27:59.195339+00'
    assert formatter.transaction_change_count == 0

    # 2nd chunk
    change = formatter._preprocess_wal2json_change(b"""{
                        "kind": "insert",
                        "schema": "public",
                        "table": "test_table",
                        "columnnames": ["uuid"],
                        "columntypes": ["int4"],
                        "columnvalues": ["00079f3e-0479-4475-acff-4f225cc5188a"]
                    }
            """)[0]
    assert change.xid == 101
    assert change.change['kind'] == 'insert'
    assert change.change['columnvalues'] == ['00079f3e-0479-4475-acff-4f225cc5188a']
    assert formatter.transaction_change_count == 1

    # 3rd chunk
    change = formatter._preprocess_wal2json_change(b""",{
                        "kind": "insert",
                        "schema": "public",
                        "table": "test_table",
                        "columnnames": ["uuid"],
                        "columntypes": ["int4"],
                        "columnvalues": ["00079f3e-0479-4475-acff-4f225cc5188a"]
                    }
            """)[0]
    assert change.xid == 101
    assert change.change['kind'] == 'insert'
    assert change.change['columnvalues'] == ['00079f3e-0479-4475-acff-4f225cc5188a']
    assert formatter.transaction_change_count == 2

    # closing chunk
    result = formatter._preprocess_wal2json_change(b"]}")
    assert result == []
    assert formatter.cur_xact == ''
    assert formatter.cur_timestamp == ''
    assert formatter.transaction_change_count == 0

    # only full_change is supported
    formatter.cur_xact = 101
    formatter.full_change = False
    with pytest.raises(ValueError) as e:
        formatter._preprocess_wal2json_change(b"""{
                            "kind": "insert",
                            "schema": "public",
                            "table": "test_table",
                            "columnnames": ["uuid"],
                            "columntypes": ["int4"],
                            "columnvalues": ["00079f3e-0479-4475-acff-4f225cc5188a"]
                        }
                """)

    # invalid states
    formatter.cur_xact = 100
    formatter.full_change = True
    with pytest.raises(ValueError) as e:
        formatter._preprocess_wal2json_change(b"""{"xid": 101,
                "timestamp": "2019-09-04 01:27:59.195339+00",
                "change": [""")

    formatter.cur_xact = ''
    with pytest.raises(ValueError) as e:
        formatter._preprocess_wal2json_change(b"""{
                        "kind": "insert",
                        "schema": "public",
                        "table": "test_table",
                        "columnnames": ["uuid"],
                        "columntypes": ["int4"],
                        "columnvalues": ["00079f3e-0479-4475-acff-4f225cc5188a"]
                    }
            """)

        formatter._preprocess_wal2json_change(b""",{
                        "kind": "insert",
                        "schema": "public",
                        "table": "test_table",
                        "columnnames": ["uuid"],
                        "columntypes": ["int4"],
                        "columnvalues": ["00079f3e-0479-4475-acff-4f225cc5188a"]
                    }
            """)
