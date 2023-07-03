
class NoSQLException(Exception):
    pass


class NotFoundException(NoSQLException):
    pass


class ConfigException(NoSQLException):
    pass


class PluginException(NoSQLException):
    pass


class ValidationException(NoSQLException):
    pass
