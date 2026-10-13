
from abc import ABC, abstractmethod

from pydantic import BaseModel 

from typing import Type

from parser import JSONStreamParser
from request import Request

class Tool[T: BaseModel](ABC):
  def __init__(
    self, 
    name: str, 
    description: str,
    input_schema: Type[T],
  ):
    self.name = name
    self.description = description
    self.input_schema = input_schema
  
  @abstractmethod
  def execute(self, props: T) -> Request | None:
    raise NotImplementedError("Subclasses must implement the execute method.")

  @abstractmethod
  def stream(self, parser: JSONStreamParser[T]):
    raise NotImplementedError("Subclasses must implement the stream method.")