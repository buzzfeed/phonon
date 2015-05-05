class PhononError(Exception):
    pass

class CacheError(PhononError):
    pass

class ConfigError(PhononError):
    pass

class ArgumentError(PhononError):
    pass

class ClientError(PhononError):
    pass

class ReadError(ClientError):
    pass

class WriteError(ClientError):
    pass

class EmptyResult(ReadError):
    pass

class NoMajority(ReadError):
    pass

class Rollback(WriteError):
    pass

class AlreadyLocked(PhononError):
    pass
