
from pydantic import BaseModel

from parser import JSONStreamParser
from request import Request
from tool import Tool

import subprocess

class Props(BaseModel):
  cmd: str
  
class Subprocess(Tool[Props]):
  def __init__(self):
    super().__init__(
      name="subprocess", 
      description="""Subprocesses with accessible I/O streams
        This module allows you to spawn processes, connect to their
        input/output/error pipes, and obtain their return codes.
        For a complete description of this module see the Python documentatio
        """,
      input_schema=Props,
    )
    
  def execute(self, props: Props):
    print(f"~ {props.cmd}")
    result = subprocess.run(props.cmd, shell=True, capture_output=True, text=True)
    print(result.stdout)
    return Request(prompt=result.stdout)

  def stream(self, parser: JSONStreamParser[Props]):
    cmd = parser.listen("cmd")
    
    if parser.is_complete("cmd"):
      result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
      print(result.stdout)
      return Request(prompt=result.stdout)
