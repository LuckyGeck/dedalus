class Enum(type):
    values = ()

    def __getattr__(cls, key):
        if key in cls.values:
            return key
        raise AttributeError(key)
