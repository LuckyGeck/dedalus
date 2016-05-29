from functools import total_ordering
import numbers
from util.config import BaseConfig, IncorrectFieldType, IncorrectFieldFormat


@total_ordering
class SymVer(BaseConfig):
    def __init__(self, major=0, minor=0, patch=0):
        if isinstance(major, int) and isinstance(minor, int) and isinstance(patch, int):
            self.version = (major, minor, patch)
        else:
            raise AttributeError('SymVer should be constructed from 3 int args')

    def __repr__(self):
        return 'v{}.{}.{}'.format(*self.version)

    def __lt__(self, other):
        return self.version < other.version

    def to_json(self):
        return repr(self)

    def from_json(self, json_doc: str, skip_unknown_fields=False, path_to_node: str = ''):
        path_to_node = self._prepare_path_to_node(path_to_node)
        if not isinstance(json_doc, str):
            raise IncorrectFieldType(
                '{}: SymVer can be constructed only from str - {} passed.'.format(path_to_node,
                                                                                  json_doc.__class__.__name__)
            )
        parts = json_doc.lstrip('v').split('.')
        if len(parts) != 3 or not all(x.isdigit() for x in parts):
            raise IncorrectFieldFormat(
                '{}: SymVer field have vN.N.N format - got {}'.format(path_to_node, json_doc)
            )
        self.version = tuple(int(x) for x in parts)
