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
        try:
            self.id = kwargs['id']
        except KeyError, e:
            raise phonon.exceptions.ArgumentError("id is a required field")

        for key, field in self.__class__._fields.items():
            setattr(self, key, kwargs[key])

    def merge(self, other):
        for key, field in self.__class__._fields.items():
            setattr(self, key, field.merge(getattr(self, key),
                                           getattr(other, key)))

    def cache(self):
        model_name = self.__class__.__name__
        for field_name, field in self.__class__._fields.items():
            field_value = getattr(self, field_name)
            if not field.cache(model_name, self.id, field_name, field_value):
                raise phonon.exceptions.CacheError("Failed to cache {}".format(field_name))
