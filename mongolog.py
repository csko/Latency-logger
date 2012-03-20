#!/usr/bin/env python2

import logging
from log4mongo.handlers import MongoHandler
from bson.timestamp import Timestamp
from config import *

class LatencyFormatter(logging.Formatter):

    DEFAULT_PROPERTIES = logging.LogRecord('', '', '', '', '', '', '', '').__dict__.keys()

    def format(self, record):
        """Formats LogRecord into python dictionary."""
        # Standard document
        document = {
            'timestamp': Timestamp(int(record.created), int(record.msecs))
        }

        # Get extra fields.
        contextual_extra = set(record.__dict__).difference(set(self.DEFAULT_PROPERTIES))
        assert contextual_extra
        for key in contextual_extra:
            document[key] = record.__dict__[key]
        return document

# TODO: ConnectionFailure

MONGOHANDLER = MongoHandler(
        host = MONGOHOST,
        port = MONGOPORT,
        database_name = MONGODB,
        collection = MONGOCOLLECTIONS["latency"],
        username = MONGOUSER,
        password = MONGOPASS,
        formatter = LatencyFormatter()
        )


class MongoLogger():
    def __init__(self):
        self.logger = logging.getLogger('_latency')
        self.logger.setLevel(logging.INFO)

        self.logger.addHandler(MONGOHANDLER)

    def log(self, src, dest, latency):
        data = {'from' : src, 'to' : dest, 'latency' : latency}
        # TODO :AutoReconnect
        self.logger.info(None, extra=data)

def get_targets():
    connection = MONGOHANDLER.connection
    db = connection.latencystats
    # TODO :AutoReconnect

    for target in db.targets.find(): # TODO: make this collection name configurable
        if target['enabled']:
            yield target['host']


