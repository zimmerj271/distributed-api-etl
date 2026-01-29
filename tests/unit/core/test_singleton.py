"""Unit tests for SingletonMeta metaclass"""
import pytest
from core import SingletonMeta


@pytest.mark.unit
class TestSingletonBasicBehavior:
    """Tests for basic singleton behavior"""
    
    def test_same_instance_returned_on_multiple_instantiations(self):
        """
        GIVEN a class using SingletonMeta
        WHEN multiple instances are created
        THEN they should all be the same object
        """
        class MySingleton(metaclass=SingletonMeta):
            def __init__(self):
                self.value = 0
        
        instance1 = MySingleton()
        instance2 = MySingleton()
        
        assert instance1 is instance2
    
    def test_init_called_only_once(self):
        """
        GIVEN a singleton class
        WHEN multiple instances are created
        THEN __init__ should only be called once
        """
        init_calls = []
        
        class MySingleton(metaclass=SingletonMeta):
            def __init__(self):
                init_calls.append(1)
                self.value = 42
        
        instance1 = MySingleton()
        instance2 = MySingleton()
        
        assert len(init_calls) == 1
        assert instance1.value == 42
        assert instance2.value == 42
    
    def test_state_persists_across_references(self):
        """
        GIVEN a singleton instance with modified state
        WHEN a new reference is created
        THEN the state should be shared
        """
        class MySingleton(metaclass=SingletonMeta):
            def __init__(self):
                self.counter = 0
        
        instance1 = MySingleton()
        instance1.counter = 100
        
        instance2 = MySingleton()
        
        assert instance2.counter == 100


@pytest.mark.unit
class TestSingletonIsolation:
    """Tests for singleton isolation between classes"""
    
    def test_different_singleton_classes_are_independent(self):
        """
        GIVEN two different classes using SingletonMeta
        WHEN instances are created
        THEN each class should have its own singleton instance
        """
        class SingletonA(metaclass=SingletonMeta):
            def __init__(self):
                self.name = "A"
        
        class SingletonB(metaclass=SingletonMeta):
            def __init__(self):
                self.name = "B"
        
        a = SingletonA()
        b = SingletonB()
        
        assert a is not b
        assert a.name == "A"
        assert b.name == "B"
    
    def test_subclasses_have_separate_instances(self):
        """
        GIVEN a singleton base class and a subclass
        WHEN instances are created
        THEN base and subclass should have separate singleton instances
        """
        class BaseSingleton(metaclass=SingletonMeta):
            def __init__(self):
                self.type = "base"
        
        class DerivedSingleton(BaseSingleton):
            def __init__(self):
                super().__init__()
                self.type = "derived"
        
        base = BaseSingleton()
        derived = DerivedSingleton()
        
        assert base is not derived
        assert base.type == "base"
        assert derived.type == "derived"


@pytest.mark.unit
class TestSingletonWithArguments:
    """Tests for singleton behavior with constructor arguments"""
    
    def test_first_instantiation_args_are_used(self):
        """
        GIVEN a singleton class that accepts constructor args
        WHEN first instance is created with args
        THEN those args should be used
        """
        class ConfigSingleton(metaclass=SingletonMeta):
            def __init__(self, name: str, value: int):
                self.name = name
                self.value = value
        
        instance = ConfigSingleton("test", 42)
        
        assert instance.name == "test"
        assert instance.value == 42
    
    def test_subsequent_instantiation_args_are_ignored(self):
        """
        GIVEN an existing singleton instance
        WHEN a new instance is created with different args
        THEN the args should be ignored (same instance returned)
        """
        class ConfigSingleton(metaclass=SingletonMeta):
            def __init__(self, name: str):
                self.name = name
        
        instance1 = ConfigSingleton("first")
        instance2 = ConfigSingleton("second")  # Args ignored
        
        assert instance1 is instance2
        assert instance2.name == "first"  # Original value


@pytest.mark.unit
class TestSingletonThreadSafety:
    """Tests for singleton thread safety (basic verification)"""
    
    def test_singleton_created_once_in_single_thread(self):
        """
        GIVEN a singleton class
        WHEN created multiple times in single thread
        THEN only one instance should exist
        
        Note: True thread-safety testing requires threading, but this
        verifies the basic mechanism works in a single thread.
        """
        class ThreadTestSingleton(metaclass=SingletonMeta):
            def __init__(self):
                self.created_count = 0
        
        instances = [ThreadTestSingleton() for _ in range(100)]
        
        # All should be the same instance
        assert all(inst is instances[0] for inst in instances)


@pytest.fixture(autouse=True)
def cleanup_singleton_instances():
    """
    Clean up singleton instances between tests to avoid pollution.
    
    This fixture runs after each test to clear the singleton registry.
    """
    yield
    
    # Clear singleton instances
    SingletonMeta._instances.clear()

