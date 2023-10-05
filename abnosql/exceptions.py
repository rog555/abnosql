
class NoSQLException(Exception):

    def __init__(self, title=None, detail=None, status=None):
        super().__init__(title)
        self.title = title
        self.detail = detail
        self.status = status

    def to_problem(self):
        return {
            'title': self.title,
            'detail': self.detail,
            'status': self.status,
            'type': None
        }


class ExistsException(NoSQLException):
    def __init__(self, title=None, detail=None, status=409):
        super(ExistsException, self).__init__(
            title or 'already exists', detail, status
        )


class NotFoundException(NoSQLException):
    def __init__(self, title=None, detail=None, status=404):
        super(NotFoundException, self).__init__(
            title or 'not found', detail, status
        )


class ConfigException(NoSQLException):
    def __init__(self, title=None, detail=None, status=400):
        super(ConfigException, self).__init__(
            title or 'invalid config', detail, status
        )


class PluginException(NoSQLException):
    def __init__(self, title=None, detail=None, status=500):
        super(PluginException, self).__init__(
            title or 'plugin exception', detail, status
        )


class ValidationException(NoSQLException):
    def __init__(self, title=None, detail=None, status=400):
        super(ValidationException, self).__init__(
            title or 'validation exception', detail, status
        )
