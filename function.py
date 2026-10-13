
from pydantic import BaseModel
from typing import Callable, Any, Type

class Function[T: BaseModel](BaseModel):
  name: str
  description: str
  execute: Callable[[T], Any]
  args_schema: Type[T]
