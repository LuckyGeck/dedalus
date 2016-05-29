import abc
import datetime
from typing import NamedTuple, Callable, Optional, TypeVar, List, Union

T = TypeVar('T')

ConfigField = NamedTuple('ConfigField', [('type', type), ('required', bool), ('default', None)])


class UnknownField(Exception):
    pass


class IncorrectFieldType(Exception):
    pass


class IncorrectFieldFormat(Exception):
    pass


class BaseConfig(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def from_json(self, json_doc: dict, skip_unknown_fields=False, path_to_node: str = ''):
        pass

    @abc.abstractmethod
    def to_json(self):
        pass

    def verify(self, path_to_node: str = ''):
        pass

    def _prepare_path_to_node(self, path_to_node):
        return self.__class__.__name__ + '.' if not path_to_node else path_to_node


def create_list_field_type(type_t: Callable[[], Union[BaseConfig, T]]) -> Callable[[], Union[BaseConfig, list]]:
    class ListConfigFieldBase(BaseConfig, list):
        _type_fabric = type_t

        def _prepare_path_to_node(self, path_to_node):
            return 'ListConfigField({})'.format(
                self._type_fabric.__name__) if not path_to_node else path_to_node.rstrip('.')

        def _verify_plain_type(self, idx, data, path_to_node: str = ''):
            if isinstance(data, self._type_fabric):
                return data
            else:
                raise IncorrectFieldType('Field {}[{}] should have type {}, but {} passed'.format(
                    path_to_node, idx, self._type_fabric.__name__, data.__class__.__name__
                ))

        def to_json(self):
            if issubclass(self._type_fabric, BaseConfig):
                return [_.to_json() for _ in self]
            else:
                return self[::]

        def from_json(self, json_list: list, skip_unknown_fields=False, path_to_node: str = ''):
            path_to_node = self._prepare_path_to_node(path_to_node)
            if json_list is not None:
                assert isinstance(json_list, list), \
                    '{}: ListConfigField can be constructed only from list'.format(path_to_node)
                self.clear()
                if issubclass(self._type_fabric, BaseConfig):
                    self.extend(
                        self._type_fabric().from_json(_, skip_unknown_fields, '{}[{}].'.format(path_to_node, idx))
                        for idx, _ in enumerate(json_list)
                    )
                else:
                    self.extend(self._verify_plain_type(idx, _, path_to_node) for idx, _ in enumerate(json_list))
            return self

        def verify(self, path_to_node: str = ''):
            path_to_node = self._prepare_path_to_node(path_to_node)
            if issubclass(self._type_fabric, BaseConfig):
                for idx, obj in enumerate(self):
                    obj.verify('{}[{}].'.format(path_to_node, idx))
            else:
                for idx, obj in enumerate(self):
                    assert isinstance(obj, self._type_fabric), 'Field {}[{}] should have type {}, but {} passed'.format(
                        path_to_node, idx, self._type_fabric.__name__, obj.__class__.__name__
                    )

    return ListConfigFieldBase


StrListConfigField = create_list_field_type(str)


def create_dict_field_type(type_t: Callable[[], T]) -> Callable[[], Union[BaseConfig, dict]]:
    class DictConfigFieldBase(BaseConfig, dict):
        _type_fabric = type_t

        def _prepare_path_to_node(self, path_to_node):
            return 'DictConfigField(str, {})'.format(
                self._type_fabric.__name__) if not path_to_node else path_to_node.rstrip('.')

        def _verify_plain_type(self, idx, data, path_to_node: str = ''):
            if isinstance(data, self._type_fabric):
                return data
            else:
                raise IncorrectFieldType('Field {}[\'{}\'] should have type {}, but {} passed'.format(
                    path_to_node, idx, self._type_fabric.__name__, data.__class__.__name__
                ))

        def to_json(self):
            return {k: v.to_json() for k, v in self.items()}

        def from_json(self, json_map: dict, skip_unknown_fields=False, path_to_node: str = ''):
            path_to_node = self._prepare_path_to_node(path_to_node)
            if json_map is not None:
                assert isinstance(json_map, dict), '{}: create_dict_field_type can be constructed only from dict'.format(
                    path_to_node)
                self.clear()
                if issubclass(self._type_fabric, BaseConfig):
                    self.update(
                        {k: self._type_fabric().from_json(v, skip_unknown_fields, '{}[\'{}\'].'.format(path_to_node, k))
                         for k, v in json_map.items()}
                    )
                else:
                    self.update({k: self._verify_plain_type(k, v, path_to_node) for k, v in json_map.items()})
            return self

        def verify(self, path_to_node: str = ''):
            path_to_node = self._prepare_path_to_node(path_to_node)
            for k, v in self.items():
                v.verify('{}[\'{}\'].'.format(path_to_node, k))
    return DictConfigFieldBase


class DateTimeField(BaseConfig):
    def __init__(self, unixtime: int = None):
        self._dt = None if unixtime is None else self.unixtime_to_datetime(unixtime)

    @staticmethod
    def datetime_to_unixtime(dt: datetime.datetime) -> int:
        return (dt - datetime.datetime(1970, 1, 1)).total_seconds()

    @staticmethod
    def unixtime_to_datetime(unixtime: 'Optional[int, float]') -> datetime.datetime:
        return datetime.datetime.fromtimestamp(int(unixtime))

    def set_to_now(self):
        self._dt = datetime.datetime.utcnow()

    def to_json(self):
        return self.datetime_to_unixtime(self._dt) if self._dt else None

    def from_json(self, unixtime: int, skip_unknown_fields=False, path_to_node: str = ''):
        path_to_node = self._prepare_path_to_node(path_to_node)
        if unixtime is None:
            self._dt = None
            return
        if not isinstance(unixtime, int) and not isinstance(unixtime, float):
            raise IncorrectFieldType(
                '{}: DateTimeField can be constructed only from int or float - {} passed.'.format(
                    path_to_node, unixtime.__class__.__name__)
            )
        self._dt = self.unixtime_to_datetime(unixtime)
        return self

    def verify(self, path_to_node: str = ''):
        path_to_node = self._prepare_path_to_node(path_to_node)
        assert isinstance(self._dt, datetime.datetime) or self._dt is None, \
            'Value of {} should be either datetime or None'.format(path_to_node)


class MetaConfig(abc.ABCMeta):
    def __new__(mcs, name, bases, nmspc):
        fields = {}
        for attr_name, attr_value in nmspc.items():
            if isinstance(attr_value, ConfigField):
                fields[attr_name] = attr_value
                nmspc[attr_name] = attr_value.default
            elif isinstance(attr_value, BaseConfig):
                fields[attr_name] = ConfigField(type=attr_value.__class__, required=True, default=attr_value)
        nmspc['_fields'] = fields
        return super().__new__(mcs, name, bases, nmspc)

    def __call__(cls, *args, **kwargs):
        obj = super(MetaConfig, cls).__call__()
        for k, v in kwargs.items():
            if k not in cls._fields:
                raise UnknownField(
                    'Attempt to set unknown config field via kwargs in ctor: field {} not found'.format(k))
            setattr(obj, k, v)
        return obj


class Config(BaseConfig, metaclass=MetaConfig):
    _fields = {}

    @classmethod
    def create(cls, json_doc: dict, skip_unknown_fields=False, verify=True):
        result = cls()
        result.from_json(json_doc=json_doc, skip_unknown_fields=skip_unknown_fields)
        if verify:
            result.verify()
        return result

    def from_json(self, json_doc: dict, skip_unknown_fields=False, path_to_node: str = ''):
        path_to_node = self._prepare_path_to_node(path_to_node)
        for k, v in json_doc.items():
            field = self._fields.get(k, None)
            if field is None:
                if skip_unknown_fields:
                    continue
                raise UnknownField('{}: Found unknown field "{}"'.format(path_to_node, k))
            if issubclass(field.type, BaseConfig):
                getattr(self, k).from_json(v, skip_unknown_fields, '{}{}.'.format(path_to_node, k))
            elif isinstance(v, field.type) or (not field.required and v is None):
                setattr(self, k, v)
            else:
                raise IncorrectFieldType(
                    'Field {}{} should have type {}, but {} passed.'.format(path_to_node, k, field.type.__name__,
                                                                            v.__class__.__name__))
        return self

    def to_json(self):
        result = {}
        for k, field in self._fields.items():
            attr_value = getattr(self, k)
            result[k] = attr_value.to_json() if issubclass(field.type, BaseConfig) else attr_value
        return result

    def verify(self, path_to_node: str = ''):
        if not path_to_node:
            path_to_node = self.__class__.__name__ + '.'
        for name, field in self._fields.items():
            if not hasattr(self, name):
                raise AttributeError('Not found attribute {}{}'.format(path_to_node, name))
            value = getattr(self, name)
            type_mismatch = not isinstance(value, field.type)
            if not field.required:
                if type_mismatch and value is not None:
                    raise AttributeError(
                        'Value for attribute {}{} should be None or of type {}, not {}'.format(
                            path_to_node, name, field.type.__name__, value.__class__.__name__
                        ))

            else:
                if type_mismatch:
                    raise AttributeError('Value for attribute {}{} is required'.format(path_to_node, name))
            if isinstance(value, BaseConfig):
                value.verify('{}{}.'.format(path_to_node, name))
