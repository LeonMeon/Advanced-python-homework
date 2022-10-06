'''
conception modularity software
The core of the system contains
only basic functionality for working with metadata and organizing the data processing, all specific
functions are placed in separate plugins
'''

from abc import abstractmethod, ABC, ABCMeta
from types import ModuleType
from typing import Optional, Any, TypeVar, Union

from .core import Named
from .meta import Meta
from .task import Task

T = TypeVar("T")


class TaskPath:
    def __init__(self, path: Union[str, list[str]]):
        if isinstance(path, str):
            self._path = path.split(".")
        else:
            self._path = path

    @property
    def is_leaf(self):
        return len(self._path) == 1

    @property
    def sub_path(self):
        return TaskPath(self._path[1:])

    @property
    def head(self):
        return self._path[0]

    @property
    def name(self):
        return self._path[-1]

    def __str__(self):
        return ".".join(self._path)


class ProxyTask(Task[T]): #The constructor of class ProxyTask receive two arguments: some task object and it proxy name

    def __init__(self, proxy_name, task: Task):
        self._name = proxy_name
        self._task = task

    @property
    def dependencies(self):
        return self._task.dependencies

    @property
    def specification(self):
        return self._task.specification

    def check_by_meta(self, meta: Meta):
        self._task.check_by_meta(meta)

    def transform(self, meta: Meta, /, **kwargs: Any) -> T:
        return self._task.transform(meta, **kwargs)


class IWorkspace(ABC, Named):

    @property
    @abstractmethod
    def tasks(self) -> dict[str, Task]:
        pass

    @property
    @abstractmethod
    def workspaces(self) -> set["IWorkspace"]:
        pass
    
    # return task from this workspace or from his sub-workspaces
    #task_path is special TaskPath object or string in next format
    def find_task(self, task_path: Union[str, TaskPath]) -> Optional[Task]:
        if not isinstance(task_path, TaskPath):
            task_path = TaskPath(task_path)
        
        # start recursion again and again and again and again
        if not task_path.is_leaf: # from TaskPath description : len(self._path) == 1
            for work_space in self.workspaces: # find appropriate head
                if work_space.name == task_path.head: # from TaskPath description  :self._path[0]
                    return work_space.find_task(task_path.sub_path) # from TaskPath description: TaskPath(self._path[1:])
            return None
        # leaf case
        else:
            for task_name in self.tasks:
                if task_name == task_path.name:
                    return self.tasks[task_name]
            # why is it possible ?....
            for work_space in self.workspaces:
                if work_space.find_task(task_path) is not None:
                    return work_space.find_task(task_path)
            return None
                
    def has_task(self, task_path: Union[str, TaskPath]) -> bool:
        return self.find_task(task_path) is not None

    def get_workspace(self, name) -> Optional["IWorkspace"]:
        for workspace in self.workspaces:
            if workspace.name == name:
                return workspace
        return None

    def structure(self) -> dict:
        return {
            "name": self.name,
            "tasks": list(self.tasks.keys()),
            "workspaces": [w.structure() for w in self.workspaces]
        }
    
    #return _stem_workspace value if it exists and module-workspace for module which contains task definition.
    @staticmethod
    def find_default_workspace(task: Task) -> "IWorkspace":  
        if hasattr(task, "_stem_workspace"): #if this variable exists, else create module-workspace and save it in this variable
            return getattr(task, "_stem_workspace")
        else: 
            return IWorkspace.module_workspace(import_module(task.__module__))
    
    # return module-workspaces:
    # instance of LocalWorkspace, which contain tasks (instance of Task or its subclasses) and workspaces (instance of IWorkspace and its subclasses) defined in module associated with variable module
    @staticmethod
    def module_workspace(module: ModuleType) -> "IWorkspace":
        if hasattr(module, "_stem_workspace"):  
            return getattr(module, "_stem_workspace")
        else:  # else create module-workspace and save it in this variable         
            tasks, workspaces = {}, []
            for attr in dir(module):
                cls = getattr(module, attr)
                if isinstance(cls, Task): # which contain tasks
                    tasks[attr] = cls
                if isinstance(cls, IWorkspace): # and workspaces
                    workspaces.append(cls)
            setattr(module, "_stem_workspace", LocalWorkspace(module.__name__, tasks, workspaces)) 
            return getattr(module, "_stem_workspace")

class ILocalWorkspace(IWorkspace):

    @property
    def tasks(self) -> dict[str, Task]:
        return self._tasks

    @property
    def workspaces(self) -> set["IWorkspace"]:
        return self._workspaces


class LocalWorkspace(ILocalWorkspace):

    def __init__(self, name,  tasks=(), workspaces=()):
        self._name = name
        self._tasks = tasks
        self._workspaces = workspaces


class Workspace(ABCMeta, ILocalWorkspace):
    def __new__(mcs, name, interfaces , attrs, **kwargs):
        cls = super().__new__(ABCMeta, name, interfaces , attrs, **kwargs) # must be returned on constructor call of user classes.
        try: 
            workspaces = set(cls.workspaces) #must be converted to set if present 
        except AttributeError:
            workspaces = set() #else property workspaces must be return empty set

        if ILocalWorkspace not in interfaces:
            interfaces += (ILocalWorkspace,) # Class-objects of user classes implement the interface ILocalWorkspace
            
        cls = super().__new__(mcs, name, interfaces, attrs, **kwargs) # must be returned on constructor call of user classes.
        cls_attr = {task_name: task for task_name, task in cls.__dict__.items()}
        
        # iii All attributes of the user workspace class must be replaced on ProxyTask objects
        cls_attr_replaced = {task_name: ProxyTask(task_name, task) for task_name, task in cls_attr.items()
            if isinstance(task, Task) and not callable(task)}
        
        for task_name, task in cls_attr_replaced.items():
            setattr(cls, task_name, task)

        for task_name, task in cls.__dict__.items():
            if isinstance(task, Task):
                task._stem_workspace = mcs # v All tasks must have it
        
        #return dictionary, which have names of tasks in keys and itself tasks in values
        tasks = {task_name: task for task_name, task in cls.__dict__.items() if isinstance(task, Task)} # iv
        
        cls._workspaces, cls._tasks, cls._name  = workspaces, tasks, name
        return cls
    

