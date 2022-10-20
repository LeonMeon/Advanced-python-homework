from enum import Enum, auto
from typing import Optional, Callable, TypeVar, Generic
from functools import cached_property
from dataclasses import dataclass, field

from .meta import Meta, MetaVerification, Specification
from .task import Task
from .workspace import Workspace
from .task_runner import TaskRunner, SimpleRunner
from .task_tree import TaskNode, TaskTree

T = TypeVar("T")


@dataclass
class TaskMetaError(Generic[T]):
    task_node: TaskNode[T]
    meta_error: Optional[MetaVerification] = None
    user_handler_error: Optional[Exception] = None
    dependencies_error: list["TaskMetaError"] = field(default_factory=list)

    @property
    def task(self) -> Task[T]:
        return self.task_node.task

    @property
    def specification(self) -> Specification:
        return self.task.specification

    @property
    def has_error(self) -> bool:
        return self.meta_error is not None or \
               any(map(lambda x: x.has_error, self.dependencies_error))


class TaskStatus(Enum):
    DEPENDENCIES_ERROR = auto()
    META_ERROR = auto()
    INVOCATION_ERROR = auto()
    CONTAINS_DATA = auto()


@dataclass
class TaskResult(Generic[T]):
    status: TaskStatus
    task_node: TaskNode[T]
    meta_errors: Optional[TaskMetaError] = None
    lazy_data: Callable[[], T] = lambda: None

    @cached_property
    def data(self) -> Optional[T]:
        try:
            return self.lazy_data()
        except Exception as e:
            self.status = TaskStatus.INVOCATION_ERROR
            raise e


class TaskMaster:

    def __init__(self, task_runner: TaskRunner[T] = SimpleRunner(), task_tree: Optional[TaskTree] = None):
        self.task_runner = task_runner
        self.task_tree = task_tree

    #This method implement next algorithm:
    def execute(self, meta: Meta, task: Task[T], workspace: Optional[Workspace] = None) -> TaskResult[T]:
        
        #1) Get the TaskNode instance for given task from existing or new task_tree.
        if self.task_tree is None:
            task_node = TaskNode(task, workspace) 
        else:            
            task_node = self.task_tree.resolve_node(task, workspace)
        
        #The method return TaskResult with TaskStatus.DEPENDENCIES_ERROR if the TaskNode instance has 
        #dependencies error.
        if task_node.has_dependence_errors:
            return TaskResult(status = TaskStatus.DEPENDENCIES_ERROR,task_node = task_node)

        #2)Verify metadata give in meta argument using MetaVerification.verify. 
        #Content of the meta argument distributed between tasks by defined rules.
        #If metadata errors is presented, should be return TaskResult with TaskStatus.META_ERROR 
        #and TaskMetaError instance.
        if task.specification is not None:
            ver = MetaVerification.verify(meta, task.specification)
            if not ver.checked_success:
                return TaskResult(status = TaskStatus.META_ERROR, task_node = task_node,
                    TaskMetaError(task_node, ver) )
            
        #3)Return TaskResult with TaskStatus.CONTAINS_DATA if dependencies or meta error is absent today.
        #In argument lazy_data must be stored callable value which run invocation of the task 
        #in the task_runner.
        return TaskResult(status = TaskStatus.CONTAINS_DATA, task_node = task_node,
                          lazy_data = lambda: self.task_runner.run(meta, task_node) )