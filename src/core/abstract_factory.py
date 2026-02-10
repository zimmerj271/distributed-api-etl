from typing import Generic, Callable, TypeVar, Hashable, ClassVar


K = TypeVar("K", bound=Hashable)
T = TypeVar("T")
CallableMap = dict[K, Callable[..., T]]
TypeMap = dict[K, type[T]]


class TypeAbstractFactory(Generic[K, T]):
    """
    Generic abstract factory that maps keys to class types.
    """

    _registry: ClassVar[TypeMap] = {}

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls._registry = {}

    @classmethod
    def register(cls, key: K) -> Callable[[type[T]], type[T]]:
        """
        Decorator for registering a concrete implementation type.
        """
        def wrapper(impl: type[T]) -> type[T]:
            cls._registry[key] = impl
            return impl

        return wrapper

    @classmethod
    def list_keys(cls) -> list[K]:
        return list(cls._registry.keys())

    @classmethod
    def create(cls, key: K, *args, **kwargs) -> T:
        return cls._registry[key](*args, **kwargs)
