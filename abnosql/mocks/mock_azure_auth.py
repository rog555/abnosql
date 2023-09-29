import os
import re

import responses  # type: ignore


def tenant_id():
    return os.environ.get(
        'AZURE_TENANT_ID', '9b833a13-cf81-4917-a5a7-7860358faeda'  # random
    )


def mock_auth():
    endpoint = 'https://login.microsoftonline.com/' + tenant_id()
    for method in ['GET', 'POST']:
        responses.add(
            getattr(responses, method),
            re.compile(r'.*login.microsoftonline.com.*'),
            json={
                'authorization_endpoint': endpoint,
                'token_endpoint': endpoint,
                'metadata': [],
                'access_token': 'foobar',
                'refresh_token': 'foobar',
                'expires_in': 100,
                'client_info': (
                    # b64 {"uid": "foo", "utid": "bar"}
                    'eyJ1aWQiOiAiZm9vIiwgInV0aWQiOiAiYmFyIn0='
                )
            },
            status=200
        )
