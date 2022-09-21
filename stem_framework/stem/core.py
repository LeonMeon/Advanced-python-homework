from typing import Optional , Protocol
from dataclasses import dataclass
import re

# hw2 6 part 1
def pascal_case_to_snake_case(name: str) -> str: 
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

#hw2 6 part 2
class Named:
    _name: Optional[str] = None

    @property
    def name(self):
        if (self._name is not None):
            return self._name
        else:
            return pascal_case_to_snake_case(self.__class__.__name__)
#hw2 1
#a protocol  as a type annotation for a dataclass object.
@dataclass(eq=True, frozen=True)
class Dataclass(Protocol):
    __dataclass_fields__: Any