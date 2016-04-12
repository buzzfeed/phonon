import phonon.fields
import phonon.exceptions


class MetaModel(type):
    def __new__(cls, name, parents, dct):
        cls._fields = {}
        for key, val in dct.items():
            if isinstance(val, phonon.fields.Field):
                cls._fields[key] = val

        return super(MetaModel, cls).__new__(cls, name, parents, dct)


class Model(object):
    __metaclass__ = MetaModel

    def __init__(self, *args, **kwargs):
        for key, field in self.__class__._fields.items():
            try:
                if hasattr(field, 'dtype'):
                    assert isinstance(kwargs[key], field.dtype)
            except KeyError, e:
                raise phonon.exceptions.PhononError("{} is a required field".format(key))
            setattr(self, key, kwargs[key])

    def merge(self, other):
        for key, field in self.__class__._fields.items():
            setattr(key, field.merge(getattr(self, key),
                                     getattr(other, key)))

    def cache(self):
        for field_name, field in self.__class__._fields.items():
            key = "{}.{}.{}".format(self.__class__.__name__, field_name, self.id)
            field.operation(key, getattr(self, field_name))
