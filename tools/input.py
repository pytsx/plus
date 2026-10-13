
from pydantic import BaseModel

from parser import JSONStreamParser
from request import Request
from tool import Tool

class Props(BaseModel):
  message: str
  
class Input(Tool[Props]):
  def __init__(self):
    super().__init__(
      name="input", 
      description="Requests user input from the console and returns it as new prompt to the agent.",
      input_schema=Props,
    )
    
  def request_input(self):
    return Request(prompt=input("\n~~> "))
    
  def execute(self, props: Props):
    print(props.message)
    return self.request_input()

  def stream(self, parser: JSONStreamParser[Props]):
    message = parser.listen("message")
    print(message, end="", flush=True)
    
    if parser.is_complete("message"):
      return self.request_input()
