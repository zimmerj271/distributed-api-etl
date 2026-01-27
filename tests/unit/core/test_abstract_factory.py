from abc import ABC, abstractmethod
from core.abstract_factory import TypeAbstractFactory


class TestClass(ABC): 

    @abstractmethod
    def get_value(self) -> int: ...


class BaseFactory(TypeAbstractFactory[str, TestClass]):
    pass


@BaseFactory.register("one")
class One(TestClass):
    def __init__(self):
        self.value = 1

    def get_value(self) -> int:
        return self.value


@BaseFactory.register("two")
class Two(TestClass):
    def __init__(self):
        self.value = 2

    def get_value(self) -> int:
        return self.value


def test_factory_registration():
    keys = BaseFactory.list_keys()
    assert set(keys) == {"one", "two"}


def test_factory_create():
    obj = BaseFactory.create("one")
    assert isinstance(obj, One)
    assert obj.get_value == 1


def test_factory_registry_isolated_per_subclass():
    class OtherFactory(TypeAbstractFactory[str, str]):
        pass

    assert OtherFactory.list_keys() == []
