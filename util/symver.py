class SymVer:
    def __init__(self, major=0, minor=0, patch=0):
        if isinstance(major, int) and isinstance(minor, int) and isinstance(patch, int):
            self.version = (major, minor, patch)
        else:
            raise AttributeError('SymVer should be constructed from 3 int args')

    def __repr__(self):
        return 'v{}.{}.{}'.format(*self.version)

    def __lt__(self, other):
        return self.version < other.version
