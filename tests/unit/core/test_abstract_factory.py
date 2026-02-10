"""Unit tests for TypeAbstractFactory pattern"""

import pytest
from abc import ABC, abstractmethod

from core.abstract_factory import TypeAbstractFactory


# Test fixtures - concrete implementations
class Animal(ABC):
    """Abstract base class for testing"""

    @abstractmethod
    def speak(self) -> str:
        """Return the sound the animal makes"""
        ...


class AnimalFactory(TypeAbstractFactory[str, Animal]):
    """Concrete factory for testing"""

    pass


@AnimalFactory.register("dog")
class Dog(Animal):
    def __init__(self, name: str = "Buddy"):
        self.name = name

    def speak(self) -> str:
        return "Woof!"


@AnimalFactory.register("cat")
class Cat(Animal):
    def __init__(self, name: str = "Whiskers"):
        self.name = name

    def speak(self) -> str:
        return "Meow!"


@pytest.mark.unit
class TestTypeAbstractFactoryRegistration:
    """Tests for factory registration mechanism"""

    def test_register_decorator_adds_class_to_registry(self):
        """
        GIVEN a TypeAbstractFactory subclass
        WHEN a class is decorated with @Factory.register(key)
        THEN it should be added to the factory's registry
        """
        keys = AnimalFactory.list_keys()

        assert "dog" in keys
        assert "cat" in keys

    def test_register_returns_original_class(self):
        """
        GIVEN the @register decorator
        WHEN applied to a class
        THEN it should return the original class unchanged
        """
        # The decorator should not modify the class
        assert Dog.__name__ == "Dog"
        assert Cat.__name__ == "Cat"

    def test_list_keys_returns_all_registered_keys(self):
        """
        GIVEN a factory with multiple registered classes
        WHEN list_keys is called
        THEN it should return all registered keys
        """
        keys = AnimalFactory.list_keys()

        assert set(keys) == {"dog", "cat"}
        assert isinstance(keys, list)


@pytest.mark.unit
class TestTypeAbstractFactoryCreation:
    """Tests for factory object creation"""

    def test_create_instantiates_registered_class(self):
        """
        GIVEN a registered class in the factory
        WHEN create is called with the key
        THEN it should return an instance of that class
        """
        dog = AnimalFactory.create("dog")

        assert isinstance(dog, Dog)
        assert isinstance(dog, Animal)

    def test_create_passes_args_to_constructor(self):
        """
        GIVEN a registered class with constructor parameters
        WHEN create is called with args
        THEN the args should be passed to the constructor
        """
        dog = AnimalFactory.create("dog", name="Max")

        assert dog.name == "Max"

    def test_create_passes_kwargs_to_constructor(self):
        """
        GIVEN a registered class with constructor parameters
        WHEN create is called with kwargs
        THEN the kwargs should be passed to the constructor
        """
        cat = AnimalFactory.create("cat", name="Mittens")

        assert cat.name == "Mittens"

    def test_create_raises_keyerror_for_unregistered_key(self):
        """
        GIVEN an unregistered key
        WHEN create is called with that key
        THEN it should raise KeyError
        """
        with pytest.raises(KeyError):
            AnimalFactory.create("bird")

    def test_created_instances_are_independent(self):
        """
        GIVEN a factory
        WHEN multiple instances are created
        THEN they should be independent objects
        """
        dog1 = AnimalFactory.create("dog", name="Max")
        dog2 = AnimalFactory.create("dog", name="Buddy")

        assert dog1 is not dog2
        assert dog1.name != dog2.name


@pytest.mark.unit
class TestTypeAbstractFactoryIsolation:
    """Tests for factory registry isolation"""

    def test_factory_registries_are_isolated_per_subclass(self):
        """
        GIVEN multiple TypeAbstractFactory subclasses
        WHEN classes are registered to each
        THEN their registries should be independent
        """

        class Vehicle(ABC):
            @abstractmethod
            def drive(self) -> str: ...

        class VehicleFactory(TypeAbstractFactory[str, Vehicle]):
            pass

        @VehicleFactory.register("car")
        class Car(Vehicle):
            def drive(self) -> str:
                return "Driving car"

        # AnimalFactory should not have vehicle keys
        assert "car" not in AnimalFactory.list_keys()

        # VehicleFactory should not have animal keys
        assert "dog" not in VehicleFactory.list_keys()
        assert "cat" not in VehicleFactory.list_keys()

        # VehicleFactory should have its own keys
        assert "car" in VehicleFactory.list_keys()

    def test_factory_subclass_has_empty_registry_initially(self):
        """
        GIVEN a new TypeAbstractFactory subclass
        WHEN no classes have been registered
        THEN list_keys should return empty list
        """

        class EmptyFactory(TypeAbstractFactory[str, str]):
            pass

        assert EmptyFactory.list_keys() == []


@pytest.mark.unit
class TestTypeAbstractFactoryWithComplexTypes:
    """Tests for factory with complex type parameters"""

    def test_factory_with_integer_keys(self):
        """
        GIVEN a factory using integer keys
        WHEN objects are registered and created
        THEN it should work correctly
        """

        class NumberedItem(ABC):
            @abstractmethod
            def get_id(self) -> int: ...

        class NumberedFactory(TypeAbstractFactory[int, NumberedItem]):
            pass

        @NumberedFactory.register(1)
        class ItemOne(NumberedItem):
            def get_id(self) -> int:
                return 1

        @NumberedFactory.register(2)
        class ItemTwo(NumberedItem):
            def get_id(self) -> int:
                return 2

        assert 1 in NumberedFactory.list_keys()
        assert 2 in NumberedFactory.list_keys()

        item = NumberedFactory.create(1)
        assert item.get_id() == 1

    def test_factory_with_enum_keys(self):
        """
        GIVEN a factory using enum keys
        WHEN objects are registered and created
        THEN it should work correctly
        """
        from enum import Enum

        class Color(str, Enum):
            RED = "red"
            BLUE = "blue"

        class ColoredItem(ABC):
            @abstractmethod
            def get_color(self) -> str: ...

        class ColorFactory(TypeAbstractFactory[Color, ColoredItem]):
            pass

        @ColorFactory.register(Color.RED)
        class RedItem(ColoredItem):
            def get_color(self) -> str:
                return "red"

        assert Color.RED in ColorFactory.list_keys()

        item = ColorFactory.create(Color.RED)
        assert item.get_color() == "red"
