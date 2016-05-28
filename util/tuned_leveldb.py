from leveldb import LevelDB as OriginalLevelDB
from json import loads, dumps
from typing import Iterator, Tuple, Optional


class LevelDB:
    def __init__(self, *args, **kwargs) -> None:
        self.db = OriginalLevelDB(*args, **kwargs)

    def collection(self, collection_name: str) -> LevelDBCollection:
        return LevelDBCollection(self, collection_name)

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


class LevelDBCollection:
    def __init__(self, db: LevelDB, collection: str):
        assert '@' not in self.collection, 'Collection name should not have Symbol \'@\' in it!'
        self.db = db
        self.collection = collection
        self.begin_key = '{}@a'.format(self.collection)
        self.data_key_prefix = '{}@b'.format(self.collection)
        self.end_key = '{}@c'.format(self.collection)

        self.db.put(self.begin_key, dict())
        self.db.put(self.end_key, dict())

    def _to_full_key(self, key: str):
        return self.data_key_prefix + key

    def _from_full_key(self, full_key: str):
        return full_key[len(self.data_key_prefix):]

    def get(self, key: str, fill_cache=True) -> dict:
        return self.db.get(self._to_full_key(key), fill_cache)

    def put(self, key: str, value: dict, sync=True):
        return self.db.put(self._to_full_key(key), value, sync)

    def iterate_all(self, include_value=True, verify_checksums=False,
                    fill_cache=True) -> 'Iterator[Tuple[str, Optional[dict]]]':
        it = self.db.db.RangeIter(key_from=self.begin_key, key_to=self.end_key,
                                  include_value=include_value,
                                  verify_checksums=verify_checksums, fill_cache=fill_cache)
        if include_value:
            for key, value in it:
                key = key.decode('utf8')
                if key not in (self.begin_key, self.end_key):
                    yield self._from_full_key(key), loads(value.decode('utf8'))
        else:
            for key in it:
                key = key.decode('utf8')
                if key not in (self.begin_key, self.end_key):
                    yield self._from_full_key(key), None
