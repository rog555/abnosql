import typing as ty


try:
    import boto3
    # import botocore.client
    # import botocore.exceptions
except ImportError:
    MISSING_DEPS = True


class Table():

    def __init__(self, name: str, config: ty.Optional[dict] = None) -> None:
        self.table_name = name
        self.table = boto3.resource('dynamodb').Table(name)
        if config is None:
            config = {}
        self.config = config
        print('dynamodb.__init__()')

    def get_item(self, **kwargs) -> ty.Dict:
        key = dict(kwargs)
        if len(key) > 2 or len(key) == 0:
            raise ValueError('get_item() must use 2 named args')
        print('key: %s' % key)
        response = self.table.get_item(
            TableName=self.table_name,
            Key=key
        )
        print('dynamodb.get_item()')
        return response

    def query(self, query: str) -> ty.Dict:
        print('dynamodb.query()')
