
class NoSQLException(Exception):
    pass  # noqa


class NotFoundException(NoSQLException):
    pass  # noqa


class ConfigException(NoSQLException):
    pass  # noqa


class PluginException(NoSQLException):
    pass  # noqa


class ValidationException(NoSQLException):
    pass  # noqa
