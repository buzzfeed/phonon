class PhononError(Exception):
    pass

class CacheError(PhononError):
    pass

class ConfigError(PhononError):
    pass

class ArgumentError(PhononError):
    pass