import logging

from nosql.table import table

__version__ = '0.0.1'


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

__all__ = [  # type: ignore
    table
]
