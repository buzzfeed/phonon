import phonon.fields
import phonon.exceptions


class MetaModel(type):
    """
    The MetaModel is a meta-class for the Model; which allows fields to be set on the model as kwargs to it's
    initializer, loosely validated against the user-defined fields.

    Validation here is extremely minimal. The Fields here are only used to provide a merge/cache interface, and to
    parse kwargs passed to the Model initializer (to be set as model members).
    """
    def __new__(cls, name, parents, dct):
        cls._fields = {}
        for key, val in dct.items():
            if isinstance(val, phonon.fields.Field):
                cls._fields[key] = val

        return super(MetaModel, cls).__new__(cls, name, parents, dct)


class Model(object):
    """
    The Model class should be the base for any user-defined models. It provides an interface to configure your
    datatypes in such a way that aggregation is simple and transparent, with global awareness. For example; if you
    define a Session model as

    ```python
    class Session(phonon.models.Model):
        id = phonon.fields.ID()
        last_10_pages_viewed = phonon.fields.WindowedList()
    ```

    You can instantiate the session on two entirely different hosts such as on host A:

    ```python
    session_one = Session(id=1, last_10_pages_viewed=[(timestamp, viewed_page)])
    ```

    And on host B:
    ```python
    session_one = Session(id=1, last_10_pages_viewed=[(timestamp, viewed_page)])
    ```
    Now each machine is aware that the global reference count for this session is _2_. This allows decisions to be made
    such as at what time to write that session data to the database; and how to sort the last ten pages viewed when in
    fact the user has viewed 20.
    """
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
