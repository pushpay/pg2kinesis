import os
import logging

FORMAT = '%(asctime)-15s %(levelname)s %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(os.getenv('PG2KINESIS_LOG_LEVEL'), logging.INFO)
