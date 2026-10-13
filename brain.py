import ollama

from typing import Any
from pydantic import BaseModel 

from tool import Tool

import json 

from pathlib import Path

client = ollama.Client()

class Decision(BaseModel):
  tool: str
  props: Any

type Prompt = str 

class Event(BaseModel):
  role: str
  tool: str
  content: dict

class Memory:
  def __init__(self, local_dir: str):
    self.local_path: Path = Path(local_dir)
    self.local_path.mkdir(parents=True, exist_ok=True)
    
    self.context_path: Path = self.local_path / "context.json"
    self.events_path: Path = self.local_path / "events.json"
    
    self.context: list[int] = []
    self.events = []
    self.load_local()
  
  def load_local(self):
    try:
      with open(self.context_path, "r") as f:
        data = json.load(f)
        self.context = data.get("context", [])
      with open(self.events_path, "r") as f:
        data = json.load(f)
        self.events = [Event.model_validate(event) for event in data.get("events", [])]
    except FileNotFoundError:
      self.context = []
      self.events = []

  def add_event(self, event: Event):
    self.events.append(event)
    with open(self.events_path, "w") as f:
      json.dump({
        "events": [event.model_dump(mode="json") for event in self.events]
      }, f, indent=2)
  
  def add_context(self, context: list[int]):
    self.context = context
    with open(self.context_path, "w") as f:
      json.dump({
        "context": self.context,
      }, f)
  
  def get_context(self) -> list[int]:
    return self.context
  
class Brain:
  def __init__(
    self, 
    model: str,
    memory: Memory
  ):
    self.model = model
    self.keep_alive = "30m"
    
    self.last_promt = ""
    
    self.memory = memory
  
  def _construct_prompt(self, 
    user_input: str, 
    tools: list[Tool]
  ):
    final_prompt =  f"""
<context>
  <instructions>
    <instruction>You are an agent that can use the following tools</instruction>
    <instruction>YOU CAN ONLY USE THE EXISTING TOOLS. DO NOT MAKE UP TOOLS</instruction>
  </instructions>
  <tools>
    { ''.join([f"""
    <{tool.name}
      description={tool.description}
      props={tool.input_schema.model_json_schema()}
    />""" 
    for tool in tools]) }
  </tools>
</context>

<prompt>
  {user_input}
</prompt>
"""
    return final_prompt

  def execute(
    self, 
    user_input: str, 
    _tools: list[Tool]
  ):
    
    self.memory.add_event(Event(
      role="user",
      tool="input",
      content={"prompt": user_input}
    ))
    
    resp = client.generate(
      self.model,
      prompt= self._construct_prompt(user_input, _tools),
      format=Decision.model_json_schema(),
      keep_alive=self.keep_alive,
      context=self.memory.get_context(),
      think=False
    )
    
    if not resp.response:
      print("No response from the model.")
      return
    
    self.memory.add_context([i for i in resp.context or []])
    decision = Decision.model_validate_json(resp.response)
    self.memory.add_event(Event(
      role="system",
      tool="decision",
      content={"prompt": user_input, "decision": decision.model_dump()}
    ))
    
    return decision
  
  def stream(self, 
    user_input: str, 
    _tools: list[Tool],
  ):
    
    resp = ""
    for part in client.generate(
      self.model,
      prompt=self._construct_prompt(user_input, _tools),
      format=Decision.model_json_schema(),
      keep_alive=self.keep_alive,
      context=self.memory.get_context(),
      think=False,
      stream=True
    ):
      if not part.response: 
        continue 
      self.memory.add_context([i for i in part.context or []])
      resp += part.response
      
      yield part.response
      
    decision = Decision.model_validate_json(resp)
    
    self.memory.add_event(Event(
      role="system",
      tool="decision",
      content={"prompt": user_input, "decision": decision.model_dump()}
    ))
