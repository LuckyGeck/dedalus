import abc

from leveldb import LevelDB as OriginalLevelDB
from json import loads, dumps
from typing import Iterator, Tuple, Optional


class DB(metaclass=abc.ABCMeta):
    def collection_view(self, collection_name: str) -> 'DB':
        return CollectionView(self, collection_name)

    @abc.abstractmethod
    def get(self, key: str, fill_cache=True) -> dict:
        pass

    @abc.abstractmethod
    def put(self, key: str, value: dict, sync=True):
        pass

    @abc.abstractmethod
    def delete(self, key: str, sync=True):
        pass

    @abc.abstractmethod
    def iterate_all(self, key_from=None, key_to=None, include_value=True, verify_checksums=False,
                    fill_cache=True) -> 'Iterator[Tuple[str, Optional[dict]]]':
        pass


class CollectionView(DB):
    def __init__(self, db: DB, collection: str):
        assert '<' not in collection and '=' not in collection and '>' not in collection, \
            'Collection name should not have Symbols \'<\', \'=\', \'>\' in it!'
        self.db = db
        self.collection = collection
        self._begin_key = '{}<'.format(self.collection)
        self._data_key_prefix = '{}='.format(self.collection)
        self._end_key = '{}>'.format(self.collection)

    def _to_full_key(self, key: str):
        return self._data_key_prefix + key

    def _from_full_key(self, full_key: str):
        return full_key[len(self._data_key_prefix):]

    def get(self, key: str, fill_cache=True) -> dict:
        return self.db.get(self._to_full_key(key), fill_cache)

    def put(self, key: str, value: dict, sync=True):
        return self.db.put(self._to_full_key(key), value, sync)

    def delete(self, key: str, sync=True):
        return self.db.delete(self._to_full_key(key), sync=sync)

    def iterate_all(self, key_from=None, key_to=None,
                    include_value=True, verify_checksums=False,
                    fill_cache=True) -> 'Iterator[Tuple[str, Optional[dict]]]':
        begin_key = self._begin_key + (key_from if key_from else '')
        end_key = self._end_key + (key_to if key_to else '')
        it = self.db.iterate_all(key_from=begin_key, key_to=end_key,
                                 include_value=include_value,
                                 verify_checksums=verify_checksums, fill_cache=fill_cache)
        for key, value in it:
            yield self._from_full_key(key), value


class LevelDB(DB):
    def __init__(self, *args, **kwargs) -> None:
        self.db = OriginalLevelDB(*args, **kwargs)

    def get(self, key: str, fill_cache=True) -> dict:
        return loads(self.db.Get(key=key.encode(), fill_cache=fill_cache).decode())

    def put(self, key: str, value: dict, sync=True):
        return self.db.Put(key=key.encode(), value=dumps(value, ensure_ascii=False).encode(), sync=sync)

    def delete(self, key: str, sync=True):
        return self.db.Delete(key=key.encode(), sync=sync)

    def iterate_all(self, key_from=None, key_to=None, include_value=True, verify_checksums=False,
                    fill_cache=True) -> 'Iterator[Tuple[str, Optional[dict]]]':
        if isinstance(key_from, str):
            key_from = key_from.encode()
        if isinstance(key_to, str):
            key_to = key_to.encode()
        it = self.db.RangeIter(key_from=key_from, key_to=key_to, include_value=include_value,
                               verify_checksums=verify_checksums, fill_cache=fill_cache)
        if include_value:
            for key, value in it:
                yield key.decode(), loads(value.decode())
        else:
            for key in it:
                yield key.decode(), None
