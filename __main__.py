
from pocket_tts import TTSModel

from request import Request
from tools.print import Print
from tools.subprocess import Subprocess
from tools.voicer import Voicer
from tools.input import Input
from tools.read_file import ReadFile
 
from brain import Brain, Memory 
from agent import Agent

model = TTSModel.load_model(language="portuguese")
voice = model.get_state_for_audio_prompt("rafael")

memory = Memory(".data")

model = Agent(
  brain=Brain(
    model="qwen3.5:4b",
    memory=memory
  ),
  tools=[
    Print(),
    Voicer(
      model=model, 
      voice=voice
    ),
    Input(),
    Subprocess(),
    ReadFile()
  ]
)

if __name__ == "__main__":
  while True:
    model.request(Request(prompt=input("\n>>> "), stream=False))