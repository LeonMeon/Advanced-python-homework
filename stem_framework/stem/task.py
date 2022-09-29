from functools import reduce
from typing import TypeVar, Union, Tuple, Callable, Optional, Generic, Any, Iterator, Iterable

from abc import ABC, abstractmethod
from .core import Named
from .meta import Specification, Meta

'''
1.Resolving the task dependencies and building the task tree.
2.Metadata verification: each task has Specification which describes required meta and the metadata processor checks input meta the correspondence to the specification.
3.The invocation of each task.
'''

T = TypeVar("T")
class Task(ABC, Generic[T], Named):
    dependencies: Tuple[Union[str, "Task"], ...]
    specification: Optional[Specification] = None
    settings: Optional[Meta] = None

    def check_by_meta(self, meta: Meta):
        pass

    @abstractmethod
    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        pass


class FunctionTask(Task[T]):
    def __init__(self, name: str, func: Callable, dependencies: Tuple[Union[str, "Task"], ...],
                 specification: Optional[Specification] = None,
                 settings: Optional[Meta] = None):
        self._name = name
        self._func = func
        self.dependencies = dependencies
        self.specification = specification
        self.settings = settings

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self._func(meta, **kwargs)


class DataTask(Task[T]):
    dependencies = ()

    @abstractmethod
    def data(self, meta: Meta) -> T:
        pass

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self.data(meta)


class FunctionDataTask(DataTask[T]):
    def __init__(self, name: str, func: Callable,
                 specification: Optional[Specification] = None,
                 settings: Optional[Meta] = None):
        self._name = name
        self._func = func
        self.specification = specification
        self.settings = settings

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)

    def data(self, meta: Meta) -> T:
        return self._func(meta)
    
#1 (2p.)
#wrap user function as FunctionDataTask object
def data(func: Callable[[Meta], T], specification: Optional[Specification] = None, **settings) -> FunctionDataTask[T]:
    return FunctionDataTask(func.__name__, func, specification, **settings)

#2 (3p.)
#wrap user function as FunctionTask object
def task(func: Callable[[Meta, ...], T], specification: Optional[Specification] = None, **settings) -> FunctionTask[T]:
    return FunctionTask(func.__name__,
                        func,
                        tuple(i for i in func.__annotations__.keys() if i != 'meta' and i != "return"),
                        specification,
                        **settings)
#3 (1p.)
#apply func for each element of the iterated dependence
class MapTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence: Union[str, "Task"]):
        self._name = "map_" + dependence.name #return the name of the dependence with the prefix "map_"
        self._func = func
        self.dependencies = dependence

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return map(self._func, *(kwargs.values()))

#4 (1p.)
#filter iterated dependence using key function
class FilterTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence: Union[str, "Task"]):
        self._name = "filter_" + dependence.name #return the name of the dependence with the prefix "filter_"
        self._func = func
        self.dependencies = dependence

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return filter(self._func, *(kwargs.values()))

#5 (1p.)
# reduce iterated dependence using func function.
class ReduceTask(Task[Iterator[T]]):
    def __init__(self, func: Callable, dependence: Union[str, "Task"]):
        self._name = "reduce_" + dependence.name #return the name of the dependence with the prefix "reduce_"
        self._func = func
        self.dependencies = dependence

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return reduce(self._func, *(kwargs.values()))














