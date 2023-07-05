import functools
from unittest.mock import patch

import botocore  # type: ignore

from nosql.mocks import query_table

ORIG_MAKE_API_CALL = botocore.client.BaseClient._make_api_call


def mock_dynamodbx(f):

    # won't need this when moto supports ExecuteStatement
    def execute_statement(kwargs):
        return {
            'Items': query_table(kwargs['Statement'], kwargs.get('Parameters'))
        }

    FUNC_MAP = {
        'ExecuteStatement': execute_statement
    }

    def _mock(self, operation_name, kwargs):
        response = None
        mock_func = FUNC_MAP.get(operation_name)
        if callable(mock_func):
            response = mock_func(kwargs)
            if response is None:
                raise Exception('mock_dynamodbx operation %s not supported' % (
                    operation_name
                ))
        else:
            response = ORIG_MAKE_API_CALL(self, operation_name, kwargs)
        return response

    @functools.wraps(f)
    def decorated(*args, **kwargs):
        _make_api_call = 'botocore.client.BaseClient._make_api_call'
        with patch(_make_api_call, _mock):
            return f(*args, **kwargs)
    return decorated
