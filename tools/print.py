
from pydantic import BaseModel

from parser import JSONStreamParser
from tool import Tool

class Props(BaseModel):
  message: str
  
class Print(Tool[Props]):
  def __init__(self):
    super().__init__(
      name="print", 
      description="Prints a message to the console.",
      input_schema=Props,
    )
    
  def execute(self, props: Props):
    print(props.message)

  def stream(self, parser: JSONStreamParser[Props]):
    message = parser.listen("message")
    if message:
      print(message, end="", flush=True)
