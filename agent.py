
from request import Request
from tool import Tool

from brain import Brain, Decision  

from parser import JSONStreamParser

class Agent: 
  def __init__(
    self, 
    brain: Brain,
    tools: list[Tool]
  ):
    self.tools = {tool.name: tool for tool in tools}
    self.brain = brain
  
  def request(self, req: Request):
    if req.stream: 
      self.stream(req.prompt)
    else:
      self.execute(req.prompt)
    
  def execute(
    self, 
    user_input: str,
  ): 
    decision = self.brain.execute(
      user_input, 
      list(self.tools.values()),
    )
    
    if not decision or not isinstance(decision, Decision):
      print("No decision made by the brain.")
      return
    
    action_tool = decision.tool
    action_props = decision.props
    
    if self.tools.get(action_tool):
      tool = self.tools[action_tool]
      tool_input = tool.input_schema.model_validate(action_props)
      try: 
        req = tool.execute(tool_input)
        if isinstance(req, Request):
          req.stream = True
          self.request(req)
        
      except Exception as e:
        print(f"Error executing tool '{action_tool}': {e}")
        
  def stream(
    self, 
    user_input: str,
  ):
    parser = JSONStreamParser(schema=Decision)
    props_parser = None
    tool = None
    
    for part in self.brain.stream(
      user_input, 
      list(self.tools.values()),
    ):
      
      if not part:
        print("No decision made by the brain.")
        continue
      
      try:
        parser.feed(part)
      except Exception as e:
        raise ValueError(f"Error parsing model output: {e}")
      
      decided_tool = parser.get("tool")
      
      if not parser.is_complete("tool"):
        continue 
      
      if decided_tool not in self.tools:
        raise ValueError(f"Tool '{decided_tool}' not found in available tools.")
      
      
      tool = self.tools[decided_tool]
      if props_parser is None:
        props_parser = parser.extract(
          key="props",
          schema=tool.input_schema
        )
      
      resp = tool.stream(props_parser)
      if isinstance(resp, Request):
        resp.stream = True
        self.request(resp)
      
    if props_parser is None:
      raise ValueError("The model stream ended without choosing a tool.")

    props_parser.finish()
    parser.finish()

    flush_and_wait = getattr(tool, "flush_and_wait", None)
    if callable(flush_and_wait):
      flush_and_wait()
