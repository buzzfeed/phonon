import tornado.ioloop
import tornado.testing

import phonon.model
import phonon.registry
import phonon.connections


class TestRegistry(tornado.testing.AsyncTestCase):

    def setUp(self):
        self.conn = phonon.connections.connect(hosts=['localhost'])
        self.conn.client.flushall()
        self.io_loop = tornado.ioloop.IOLoop.current()
        self.io_loop.clear_instance()

        phonon.registry.registry = phonon.registry.Registry()

    def tearDown(self):
        phonon.registry.clear()
        phonon.registry.registry = phonon.registry.Registry()

    def test_registry_stores_new_models(self):
        class Bar(phonon.model.Model):
            def on_complete(self):
                pass
        model = Bar(id=1)
        phonon.registry.register(model)
        assert phonon.registry.registry.models['Bar.1'] is model

    def test_registry_stores_timeout(self):
        class Bar(phonon.model.Model):
            def on_complete(self):
                pass
        model = Bar(id=1)
        phonon.registry.register(model)
        assert isinstance(phonon.registry.registry.timeouts['Bar.1'], tornado.ioloop._Timeout)

    def test_on_expire_gets_called_as_well_as_dereference(self):
        class DerefTest(phonon.model.Model):
            TTL = 0.1

            def __init__(self, *args, **kwargs):
                class Reference(object):

                    def __init__(self):
                        self.called = False

                    def dereference(self, *args, **kwargs):
                        self.called = True
                        tornado.ioloop.IOLoop.current().stop()

                super(DerefTest, self).__init__(*args, **kwargs)
                self.reference = Reference()
                self.completed = False

        model = DerefTest(id=1)
        phonon.registry.register(model)
        tornado.ioloop.IOLoop.current().start()
        assert model.reference.called

    def test_on_complete_gets_called(testcase):
        class OnCompleteTest(phonon.model.Model):
            TTL = 0.1

            def __init__(self, *args, **kwargs):
                super(OnCompleteTest, self).__init__(*args, **kwargs)
                self.completed = False

            def on_complete(self):
                self.completed = True
                testcase.stop()

        model = OnCompleteTest(id=1)
        phonon.registry.register(model)
        testcase.wait()
        assert model.completed, "OnCompleteModel's `on_complete` was not called"

    def test_configure_sets_max_entries(self):
        phonon.registry.configure(max_entries=12)
        assert phonon.registry.registry.max_entries == 12

    def test_expire_forces_expiry(testcase):
        class ForceExpiryTest(phonon.model.Model):
            TTL = 0.1
            id = phonon.fields.ID()
            foo = phonon.fields.Sum()
            bar = phonon.fields.Sum()

            def __init__(self, *args, **kwargs):
                super(ForceExpiryTest, self).__init__(*args, **kwargs)
                self.completed = False

            def on_complete(self):
                self.completed = True

        conn_1 = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        phonon.connections.connection = conn_1
        model_1 = ForceExpiryTest(id=1, foo=1, bar=2, __init_cache=True)

        registry_1 = phonon.registry.Registry()
        registry_1.register(model_1)

        conn_2 = phonon.connections.AsyncConn(redis_hosts=['localhost'])
        phonon.connections.connection = conn_2
        model_2 = ForceExpiryTest(id=1, foo=9, bar=18, __init_cache=True)
        registry_2 = phonon.registry.Registry()
        registry_2.register(model_2)

        registry_1.expire(model_1)
        registry_2.expire(model_2)
