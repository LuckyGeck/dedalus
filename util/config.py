from typing import NamedTuple

ConfigField = NamedTuple('ConfigField', [('type', type), ('required', bool), ('default', None)])


class UnknownField(Exception):
    pass


class IncorrectFieldType(Exception):
    pass


class BaseConfig:
    pass


class MetaConfig(type):
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

    def from_json(self, json_doc: dict, skip_unknown_fields=False):
        for k, v in json_doc.items():
            field = self._fields.get(k, None)
            if field is None:
                if skip_unknown_fields:
                    continue
                raise UnknownField('Found unknown field "{}"'.format(k))
            if issubclass(field.type, Config):
                getattr(self, k).from_json(v, skip_unknown_fields)
            else:
                if isinstance(v, field.type):
                    setattr(self, k, v)
                else:
                    raise IncorrectFieldType(
                        'Field {} should have type {}, but {} passed.'.format(k, field.type.__name__,
                                                                              v.__class__.__name__))

    def to_json(self):
        result = {}
        for k, field in self._fields.items():
            attr_value = getattr(self, k)
            result[k] = attr_value.to_json() if issubclass(field.type, Config) else attr_value
        return result

    def verify(self):
        for name, field in self._fields.items():
            if not hasattr(self, name):
                raise AttributeError('Not found attribute {}'.format(name))
            value = getattr(self, name)
            if not isinstance(value, field.type):
                raise AttributeError(
                    'Value for attribute {} should be of type {}, not {}'.format(name, field.type.__name__,
                                                                                 value.__class__.__name__))
            if field.required and value is None:
                raise AttributeError('Value for attribute {} is required'.format(name))