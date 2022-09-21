'''
conception of metadata and metadata processor
metadata - user-defined tree of values (to cache analysis
results or send instructions to remote computing node)
metadata processor principle - during the
processing of data, only immutable initial data and metadata are allowed to be used as input -> declarative
approach of describing data (no scripts, processing description in the form of metadata)
'''
from dataclasses import dataclass
from .core import Dataclass
from typing import Optional, Any, Union

#Meta --- union of the dict and the Dataclass type.
Meta : Union[dict, Dataclass]

#Specification field type SpecificationField --- pairs of necessary meta key and necessary meta value type (this can be single type, tuple of types, or another specification if meta value is another Meta)
SpecificationField = dict[Meta, Union[Any, tuple[Any, ...]], dict[Meta, Union[Any, tuple[Any, ...]]]]

#Specification for specification --- union of the Dataclass or tuple of the SpecifiationField type. Note: we can add additional type annotations if necessary (so I should add ...).
Specification = Union[Dataclass, tuple[SpecificationField, ...]]



class SpecificationError(Exception):
    pass # do I need smth here?


@dataclass
class MetaFieldError:
    required_key: str
    required_types: Optional[tuple[type]] = None
    presented_type: Optional[type] = None
    presented_value: Any = None


#3.(1 p.) get_meta_attr(meta : Meta, key : str, default : Optional[Any] = None) -> Optional[Any]: which return meta value by key from top level of meta or default if key don't exist in meta
def get_meta_attr(meta : Meta, key : str, default : Optional[Any] = None) -> Optional[Any]:
    if hasattr(meta, key):
        return getattr(meta, key)
    return default

#4.(1 p.) function def update_meta(meta: Meta, **kwargs): which update meta from kwargs.
def update_meta(meta: Meta, **kwargs):
    for key in kwargs:
        setattr(meta, key, kwargs[key])  
        
#5.(6 p.) this class contains result of meta verification.
#It contains list of instance of dataclass MetaFieldError or another MetaVerification in the field errors:
class MetaVerification:

    def __init__(self, *errors: Union[MetaFieldError, MetaVerification]):
        self.error = errors
        pass

    #returns True if there is no errors of verification.
    @property
    def checked_success(self):
        if no_errors:
            return True
        else:
            return False
        
    # verify meta by specification. Raise SpecificationError if verification impossible.
    @staticmethod
    def verify(meta: Meta,
               specification: Optional[Specification] = None) -> "MetaVerification":
        try:
            
        except:
            raise SpecificationError
