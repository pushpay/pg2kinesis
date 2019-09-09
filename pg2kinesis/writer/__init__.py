from .log import LogWriter
from .firehose import FirehoseWriter


WRITERS = {
    'log': LogWriter,
    'firehose': FirehoseWriter
}

def get_writer(writer, name, send_window=15):
    writer = writer.lower()
    if writer not in WRITERS:
        raise ValueError('%s is not a valid writer' % writer)
    if writer == 'log':
        name = 'pg2kinesis.writer.log.' + name
    return WRITERS[writer](name, send_window=send_window)
