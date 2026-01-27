from typing import Any


class SingletonMeta(type):
    """
    A metaclass that implements the Singleton design pattern.
    
    Classes using this metaclass can only have one instance. The first
    instantiation creates the instance, and all subsequent instantiation
    attempts return the same instance.
    
    Example:
        class Singleton(metaclass=SingletonMeta):
            def __init__(self):
                self.connection = None
        
        s1 = Singleton()  # Creates new instance
        s2 = Singleton()  # Returns the same instance
        assert s1 is s2   # True
    """
    _instances: dict[type, Any] = {} 

    def __call__(cls, *args, **kwargs) -> Any:
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]
