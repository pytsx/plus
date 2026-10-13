
from pydantic import BaseModel

from parser import JSONStreamParser

from tool import Tool

from request import Request

from pathlib import Path

class Props(BaseModel):
  filepath: str
  
class ReadFile(Tool[Props]):
  def __init__(self):
    super().__init__(
      name="read_file", 
      description="Reads a file consume its content",
      input_schema=Props,
    )
    
  def execute(self, props: Props):
    filepath = Path(props.filepath)
    
    if not filepath.is_file():
      print("Error: File not found.")
      return

    content = filepath.read_text()
    print(content)
    return Request(prompt=content, stream=True)

  def stream(self, parser: JSONStreamParser[Props]):
    if parser.is_complete("filepath"):
      filepath = Path(parser.get("filepath"))
      
      if not filepath.is_file():
        print("Error: File not found.")
        return
  
      content = filepath.read_text()
      print(content)
      return Request(prompt=content, stream=True)