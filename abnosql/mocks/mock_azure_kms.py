import functools
import json
import os
import re
import time
from urllib import parse as urlparse

import responses  # type: ignore

# this is used because don't want to poke around in internals
# of azure.keyvault.keys and use RsaKey to wrap/unwrap (and
# also couldnt get it to work)
AESGCM_KEY = 'qaIQJKZjpemmOBhKvEln6w=='  # some random made up b64 AESGCM key
AUTH_COUNTER = 0


def tenant_id():
    return os.environ.get(
        'AZURE_TENANT_ID', '9b833a13-cf81-4917-a5a7-7860358faeda'  # random
    )


def mock_azure_kms(f):

    def _callback(request):
        path = urlparse.urlsplit(request.url).path
        method = request.method
        parsed = urlparse.urlparse(request.url)
        host = f'{parsed.scheme}://{parsed.netloc}'
        # print(f'REQ: {method} {request.url} H: {request.headers} B: {request.body}')  # noqa

        def _response(code=404, body=None, _headers=None):
            if code == 404:
                body = {
                    'error': {
                        'code': 'KeyNotFound',
                        'message': 'key not found'
                    }
                }
            if body is not None:
                body = json.dumps(body)
            # print(f'RESP: {body}')
            return code, _headers or {}, body

        parts = [_ for _ in path.split('/') if _ != '']

        kid = None
        if len(parts) >= 3 and parts[0] == 'keys':
            kid = host + '/' + '/'.join(parts[0:3])
            kid = kid.split('?')[0]

        epoch = int(time.time())

        # /keys/bar/45e36a1024a04062bd489db0d9004d09
        if method == 'GET' and len(parts) == 3 and parts[0] == 'keys':
            global AUTH_COUNTER
            AUTH_COUNTER += 1
            if AUTH_COUNTER == 1:
                # without this, keys auth challenge strips the POST body
                auth = 'https://login.microsoftonline.com/' + tenant_id()
                res = 'https://vault.azure.net'
                headers = {
                    'WWW-Authenticate': (
                        f'Bearer authorization="{auth}", resource="{res}"'
                    )
                }
                return _response(401, {}, headers)
            AUTH_COUNTER = 0
            # response from https://learn.microsoft.com/en-us/rest/api/keyvault/keys/get-key/get-key  # noqa
            return _response(
                200,
                {
                    'key': {
                        'kid': kid,
                        'kty': 'RSA',
                        'key_ops': [
                            'encrypt',
                            'decrypt',
                            'sign',
                            'verify',
                            'wrapKey',
                            'unwrapKey'
                        ],
                        # this is from get-key documentation, so public key
                        # returned in the JWK won't actually match below
                        # wrap/unwrap
                        'n': '2HJAE5fU3Cw2Rt9hEuq-F6XjINKGa-zskfISVqopqUy60GOs2eyhxbWbJBeUXNor_gf-tXtNeuqeBgitLeVa640UDvnEjYTKWjCniTxZRaU7ewY8BfTSk-7KxoDdLsPSpX_MX4rwlAx-_1UGk5t4sQgTbm9T6Fm2oqFd37dsz5-Gj27UP2GTAShfJPFD7MqU_zIgOI0pfqsbNL5xTQVM29K6rX4jSPtylZV3uWJtkoQIQnrIHhk1d0SC0KwlBV3V7R_LVYjiXLyIXsFzSNYgQ68ZjAwt8iL7I8Osa-ehQLM13DVvLASaf7Jnu3sC3CWl3Gyirgded6cfMmswJzY87w',  # noqa
                        'e': 'AQAB'
                    },
                    'attributes': {
                        'created': epoch,
                        'updated': epoch
                    },
                    'tags': {}
                }
            )

        # /keys/bar/45e36a1024a04062bd489db0d9004d09/unwrapkey
        # /keys/bar/45e36a1024a04062bd489db0d9004d09/wrapkey
        elif method == 'POST' and len(parts) == 4 and 'wrap' in parts[-1]:
            if parts[-1] == 'wrapkey':
                # response from https://learn.microsoft.com/en-us/rest/api/keyvault/keys/unwrap-key/wrap-key  # noqa
                return _response(
                    200,
                    {
                        'kid': kid,
                        # rsa wrapped of AESGCM key, tho sdk doesnt call
                        # wrapkey because its wrapped by SDK client side
                        # using CMK RSA public key
                        'value': 'g6omIiikuF9OnNRmJlj6+hLe4sKC6c/94kfluSa6mZx9KiGPvlyvQXq6AcqQpXU1co6JoG7Numq4YCrZiAqzHpyyMMFrTuostGlWA3py9CwW9TLFFYNXzozwrBTbg32De4DPq5EiWvmLGjOVktEPKDz44ZgO49jrKljcJCpdVHdSYJKHy2XyV7UO/Xik463UAT19c/4ObGRb9yXylcMR5oayArAJuxJV2MPeM4BaZapU/rhrLAOLNEcVTSKGkhBc6zXBdKsznhZJ9C6vm53eUDZjgFgaMARMKg0VZJELYi47Cuxanlz41GTVj35f5rxq1c103exHZ5b79cR0f7LqQA=='  # noqa
                    }
                )
            elif parts[-1] == 'unwrapkey':
                # response from https://learn.microsoft.com/en-us/rest/api/keyvault/keys/unwrap-key/unwrap-key  # noqa
                return _response(
                    200,
                    {
                        'alg': 'RSAOAEP256',
                        # some random made up AESGCM key
                        'value': AESGCM_KEY
                    }
                )

        return _response(404)

    @functools.wraps(f)
    def decorated(*args, **kwargs):
        for method in ['GET', 'POST']:
            responses.add_callback(
                getattr(responses, method),
                re.compile(r'^https://.*.vault.azure.net.*'),
                _callback,
                content_type='application/json'
            )
            endpoint = 'https://login.microsoftonline.com/' + tenant_id()
            # this is not what azure would return, but it works to
            # mock the auth pipeline and allow POST body to reach
            # the mock rather than be stripped without the auth challenge
            # (not that mock actually uses POST body...)
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
        return f(*args, **kwargs)
    return decorated
