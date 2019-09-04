from .log import LogWriter


WRITERS = {
    'log': LogWriter
}

def get_writer(writer, name):
    writer = writer.lower()
    if writer not in WRITERS:
        raise ValueError('%s is not a valid writer' % writer)
    if writer == 'log':
        name = 'pg2kinesis.writer.log.' + name
    return WRITERS[writer](name)
