from leveldb import LevelDB as OriginalLevelDB
from json import loads, dumps


class LevelDB:
    def __init__(self, *args, **kwargs) -> None:
        self.db = OriginalLevelDB(*args, **kwargs)

    def get(self, key: str, fill_cache=True) -> dict:
        return loads(self.db.Get(key=key.encode('utf8'), fill_cache=fill_cache).decode('utf8'))

    def put(self, key: str, value: dict, sync=True):
        return self.db.Put(key=key.encode('utf8'), value=dumps(value, ensure_ascii=False).encode('utf8'), sync=sync)

    def iterate_all(self, include_value=True, verify_checksums=False,
                    fill_cache=True) -> 'Iterator[Tuple[str, Optional[dict]]]':
        it = self.db.RangeIter(include_value=include_value, verify_checksums=verify_checksums, fill_cache=fill_cache)
        if include_value:
            for key, value in it:
                yield key.decode('utf8'), loads(value.decode('utf8'))
        else:
            for key in it:
                yield key.decode('utf8'), None
