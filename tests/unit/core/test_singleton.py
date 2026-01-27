from core.singleton import SingletonMeta


class MySingleton(metaclass=SingletonMeta):
    def __init__(self):
        self.value = 0


def test_singleton_instance_identity():
    a = MySingleton()
    b = MySingleton()

    assert a is b


def test_singleton_init_only_once():
    a = MySingleton()
    a.value = 42

    b = MySingleton()
    assert b.value == 42

