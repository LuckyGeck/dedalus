import abc
from typing import NamedTuple

ConfigField = NamedTuple('ConfigField', [('type', type), ('required', bool), ('default', None)])


class UnknownField(Exception):
    pass


class IncorrectFieldType(Exception):
    pass


class IncorrectFieldFormat(Exception):
    pass


class BaseConfig(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def from_json(self, json_doc: dict, skip_unknown_fields=False):
        pass

    @abc.abstractmethod
    def to_json(self):
        pass

    def verify(self, path_to_node: str = ''):
        pass


class ListConfigField(BaseConfig):
    def __init__(self, nested_type_fabric: 'Callable[[], BaseConfig]'):
        self._type_fabric = nested_type_fabric
        self.objects = list()

    def to_json(self):
        return [_.to_json() for _ in self.objects]

    def from_json(self, json_doc: dict, skip_unknown_fields=False):
        if json_doc is not None:
            assert isinstance(json_doc, list), 'ListConfigField can be constructed only from list'
            self.objects = [self._type_fabric().from_json(_) for _ in json_doc]
        return self

    def verify(self, path_to_node: str = ''):
        if not path_to_node:
            path_to_node = '{}({}).'.format(self.__class__.__name__, self._type_fabric.__class__.name)
        for idx, obj in enumerate(self.objects):
            obj.verify('{}[{}].'.format(path_to_node, idx))


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

    def from_json(self, json_doc: dict, skip_unknown_fields=False):
        for k, v in json_doc.items():
            field = self._fields.get(k, None)
            if field is None:
                if skip_unknown_fields:
                    continue
                raise UnknownField('Found unknown field "{}"'.format(k))
            if issubclass(field.type, BaseConfig):
                getattr(self, k).from_json(v, skip_unknown_fields)
            elif isinstance(v, field.type) or (not field.required and v is None):
                setattr(self, k, v)
            else:
                raise IncorrectFieldType(
                    'Field {} should have type {}, but {} passed.'.format(k, field.type.__name__,
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
