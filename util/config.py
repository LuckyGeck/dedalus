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
    def __init__(self, **kwargs):
        self._parent = kwargs.get('parent_object')  # type: BaseConfig
        self._parent_key = kwargs.get('parent_key')  # type: str

    def set_parent(self, parent_object: 'BaseConfig', parent_key: str) -> 'BaseConfig':
        self._parent = parent_object
        self._parent_key = parent_key
        return self

    def get_path_to_child(self, child_key: str):
        return '{}.{}'.format(self.path_to_node, child_key)

    @property
    def path_to_node(self) -> str:
        if self._parent:
            return self._parent.get_path_to_child(self._parent_key)
        else:
            return self.get_class_name()

    @classmethod
    def get_class_name(cls):
        return cls.__name__

    @abc.abstractmethod
    def from_json(self, json_doc: dict, skip_unknown_fields=False):
        pass

    @abc.abstractmethod
    def to_json(self):
        pass

    def verify(self):
        pass


def create_list_field_type(type_t: Callable[[], Union[BaseConfig, T]]) -> Callable[[], BaseConfig]:
    class ListConfigFieldBase(BaseConfig, list):
        _type_fabric = type_t

        @classmethod
        def get_class_name(cls):
            return 'ListConfigField({})'.format(cls._type_fabric.__name__)

        def get_path_to_child(self, child_key: str):
            return '{}[{}]'.format(self.path_to_node, child_key)

        def _verify_plain_type(self, idx, data):
            if isinstance(data, self._type_fabric):
                return data
            else:
                raise IncorrectFieldType('Field {}[{}] should have type {}, but {} passed'.format(
                    self.path_to_node, idx, self._type_fabric.__name__, data.__class__.__name__
                ))

        def to_json(self):
            if issubclass(self._type_fabric, BaseConfig):
                return [_.to_json() for _ in self]
            else:
                return self[::]

        def from_json(self, json_list: list, skip_unknown_fields=False):
            if json_list is not None:
                assert isinstance(json_list, list), \
                    '{}: ListConfigField can be constructed only from list'.format(self.path_to_node)
                self.clear()
                if issubclass(self._type_fabric, BaseConfig):
                    self.extend(
                        self._type_fabric(parent_object=self, parent_key=str(idx))
                            .from_json(_, skip_unknown_fields)
                        for idx, _ in enumerate(json_list)
                    )
                else:
                    self.extend(self._verify_plain_type(idx, _) for idx, _ in enumerate(json_list))
            return self

        def verify(self):
            if issubclass(self._type_fabric, BaseConfig):
                for obj in self:
                    obj.verify()
            else:
                for idx, obj in enumerate(self):
                    assert isinstance(obj, self._type_fabric), 'Field {} should have type {}, but {} passed'.format(
                        self.get_path_to_child(str(idx)), self._type_fabric.__name__, obj.__class__.__name__
                    )

    return ListConfigFieldBase


StrListConfigField = create_list_field_type(str)


def create_dict_field_type(type_t: Callable[[], T]) -> Callable[[], BaseConfig]:
    class DictConfigFieldBase(BaseConfig, dict):
        _type_fabric = type_t

        @classmethod
        def get_class_name(cls):
            return 'DictConfigField(str -> {})'.format(cls._type_fabric.__name__)

        def get_path_to_child(self, child_key: str):
            return '{}[{}]'.format(self.path_to_node, child_key)

        def _verify_plain_type(self, key, data):
            if isinstance(data, self._type_fabric):
                return data
            else:
                raise IncorrectFieldType('Field {} should have type {}, but {} passed'.format(
                    self.get_path_to_child(key), self._type_fabric.__name__, data.__class__.__name__
                ))

        def to_json(self):
            return {k: v.to_json() for k, v in self.items()}

        def from_json(self, json_map: dict, skip_unknown_fields=False):
            if json_map is not None:
                assert isinstance(json_map, dict), \
                    '{}: create_dict_field_type can be constructed only from dict'.format(self.path_to_node)
                self.clear()
                if issubclass(self._type_fabric, BaseConfig):
                    self.update({
                        k: self._type_fabric(parent_object=self, parent_key=k).from_json(v, skip_unknown_fields)
                        for k, v in json_map.items()
                    })
                else:
                    self.update({k: self._verify_plain_type(k, v) for k, v in json_map.items()})
            return self

        def verify(self):
            if issubclass(self._type_fabric, BaseConfig):
                for obj in self.values():
                    obj.verify()
            else:
                for key, obj in self.items():
                    assert isinstance(obj, self._type_fabric), 'Field {} should have type {}, but {} passed'.format(
                        self.get_path_to_child(key), self._type_fabric.__name__, obj.__class__.__name__
                    )
    return DictConfigFieldBase


class DateTimeField(BaseConfig):
    def __init__(self, unixtime: int = None, **kwargs):
        super().__init__(**kwargs)
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

    def from_json(self, unixtime: int, skip_unknown_fields=False):
        if unixtime is None:
            self._dt = None
            return
        if not isinstance(unixtime, int) and not isinstance(unixtime, float):
            raise IncorrectFieldType(
                '{}: DateTimeField can be constructed only from int or float - {} passed.'.format(
                    self.path_to_node, unixtime.__class__.__name__)
            )
        self._dt = self.unixtime_to_datetime(unixtime)
        return self

    def verify(self):
        assert isinstance(self._dt, datetime.datetime) or self._dt is None, \
            '{}: DateTimeField should be either datetime or None, but it is {}'.format(self.path_to_node,
                                                                                       self._dt.__class__.__name__)


class MetaConfig(abc.ABCMeta):
    def __new__(mcs, name, bases, nmspc):
        fields = {}

        for attr_name, attr_value in nmspc.items():
            if isinstance(attr_value, ConfigField):
                fields[attr_name] = attr_value
            elif isinstance(attr_value, BaseConfig):
                fields[attr_name] = ConfigField(type=attr_value.__class__, required=True, default=None)
        nmspc['_fields'] = fields
        return super().__new__(mcs, name, bases, nmspc)

    def __call__(cls, *args, **kwargs):
        obj = super(MetaConfig, cls).__call__(*args, **kwargs)
        for k, v in cls._fields.items():
            if issubclass(v.type, BaseConfig):
                setattr(obj, k, v.type(parent_object=obj, parent_key=k))
            else:
                setattr(obj, k, v.default)
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

    def from_json(self, json_doc: dict, skip_unknown_fields=False):
        for k, v in json_doc.items():
            field = self._fields.get(k, None)
            if field is None:
                if skip_unknown_fields:
                    continue
                raise UnknownField('{}: Found unknown field "{}"'.format(self.path_to_node, k))
            if issubclass(field.type, BaseConfig):
                getattr(self, k).from_json(v, skip_unknown_fields)
            elif isinstance(v, field.type) or (not field.required and v is None):
                setattr(self, k, v)
            else:
                raise IncorrectFieldType(
                    'Field {} should have type {}, but {} passed.'.format(self.get_path_to_child(k),
                                                                          field.type.__name__, v.__class__.__name__))
        return self

    def to_json(self):
        result = {}
        for k, field in self._fields.items():
            attr_value = getattr(self, k)
            result[k] = attr_value.to_json() if issubclass(field.type, BaseConfig) else attr_value
        return result

    def verify(self):
        for name, field in self._fields.items():
            if not hasattr(self, name):
                raise AttributeError('Not found attribute {}'.format(self.get_path_to_child(name)))
            value = getattr(self, name)
            type_mismatch = not isinstance(value, field.type)
            if not field.required:
                if type_mismatch and value is not None:
                    raise AttributeError(
                        'Value for attribute {} should be None or of type {}, not {}'.format(
                            self.get_path_to_child(name), field.type.__name__, value.__class__.__name__
                        ))

            else:
                if type_mismatch:
                    raise AttributeError('Value for attribute {} is required'.format(self.get_path_to_child(name)))
            if isinstance(value, BaseConfig):
                value.verify()
