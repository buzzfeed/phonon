from phonon.connections import connect
connect(hosts=['localhost'])

from phonon.fields import *
from phonon.model import Model

class Foo(Model):
    a = SumField()

foo = Foo(a=1)
print Foo._fields
print foo.a
