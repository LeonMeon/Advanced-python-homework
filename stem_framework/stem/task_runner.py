import os
from typing import Generic, TypeVar
from abc import ABC, abstractmethod

from .meta import Meta
from .task_tree import TaskNode

T = TypeVar("T")


class TaskRunner(ABC, Generic[T]):

    @abstractmethod
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass


class SimpleRunner(TaskRunner[T]):
    #his method run the method task_node.task.transform
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        assert not task_node.has_dependence_errors
        _dict = {d.task.name: d.task.transform(meta) for d in task_node.dependencies} 
        return task_node.task.transform(meta, **_dict)


class ThreadingRunner(TaskRunner[T]):
    MAX_WORKERS = 5

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)


class AsyncRunner(TaskRunner[T]):
    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)


class ProcessingRunner(TaskRunner[T]):
    MAX_WORKERS = os.cpu_count()

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        pass  # TODO(Assignment 9)