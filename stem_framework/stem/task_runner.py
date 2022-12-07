import os
import asyncio
from typing import Generic, TypeVar
from abc import ABC, abstractmethod
from concurrent import futures
from multiprocessing.pool import ThreadPool 
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

#which execute every task in own thread
# Use MAX_WORKERS class field as maximum number of threads that can be used to execute.
class ThreadingRunner(TaskRunner[T]):
    MAX_WORKERS = 5

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        deps = task_node.dependencies
        attrs = [get_meta_attr(meta, dep.task.name, {}) for dep in deps]
        
        with futures.ThreadPoolExecutor(max_workers = ThreadingRunner.MAX_WORKERS) as ex:
            maps = list(ex.map(self.run, attrs, deps))
        
        #maps_dict = {}
        #for i, dep in enumerate(deps): 
        #    maps_dict[dep.task.name] =  maps[i]
        maps_dict = {deps[i].task.name: maps[i] for i in range(len(maps))}
        return task_node.task.transform(meta, **maps_dict)

#which execute every task in own process.
#Use MAX_WORKERS class field as maximum number of processes that can be used to execute.
class ProcessingRunner(TaskRunner[T]):
    MAX_WORKERS = os.cpu_count()

    def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        deps = task_node.dependencies
        attrs = [get_meta_attr(meta, dep.task.name, {}) for dep in deps]
        
        with ThreadPool(ProcessingRunner.MAX_WORKERS) as Pool:
            starmaps = Pool.starmap(self.run, list(zip(attrs, deps)))
            
        starmaps_dict = {deps[i].task.name: starmaps[i] for i in range(len(starmaps))}
        return task_node.task.transform(meta, **starmaps_dict)    
    
    
#which execute every task in own coroutine
class AsyncRunner(TaskRunner[T]):
    async def run(self, meta: Meta, task_node: TaskNode[T]) -> T:
        #attrs = [get_meta_attr(meta, dep.task.name, {}) for dep in deps]
        #run_dict = {dep.task.name : await self.run(attr, dep) for dep,attr in zip(deps, attrs)}
        deps = task_node.dependencies
        _dict = {dep.task.name: await self.run(get_meta_attr(meta, dep.task.name, {}), dep) for dep in deps}
        return task_node.task.transform(meta, **_dict)

