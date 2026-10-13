import re
import queue
import threading

from pydantic import BaseModel
import sounddevice as sd

from tool import Tool
from parser import JSONStreamParser

class VoiceInput(BaseModel):
  message: str
  
  
class Voicer(Tool[VoiceInput]):
  def __init__(self, model, voice, events=None):
    super().__init__(
      name="voicer", 
      description="Converts text to speech and plays it through the audio output.",
      input_schema=VoiceInput,
    )
    
    self.model = model
    self.voice = voice
    self.events = events or queue.Queue()

    self.buffer = ""

    self.text_queue = queue.Queue()
    self.audio_queue = queue.Queue(maxsize=32)

    self.tts_thread = threading.Thread(
      target=self._synthesize
    )

    self.audio_thread = threading.Thread(
      target=self._play
    )

    self.tts_thread.start()
    self.audio_thread.start()
  
  def stream(self, parser: JSONStreamParser[VoiceInput]):
    msg = parser.listen("message")
    if  msg and isinstance(msg, str):
      print(msg, end="", flush=True)
      self._process(msg)
    
  def execute(self, props: VoiceInput):
    self._process(props.message)

  def flush_and_wait(self):
    """Reproduz o restante da resposta atual antes de aceitar outra."""
    if self.buffer.strip():
      self.text_queue.put(("text", self.buffer.strip()))
      self.buffer = ""

    done = threading.Event()
    self.text_queue.put(("flush", done))
    done.wait()

  def _process(self, part: str):
    self.buffer += part

    while True:
      match = re.search(r"[.!?;:]\s+", self.buffer)

      if match:
        cut = match.end()
      elif len(self.buffer.split()) >= 12:
        cut = self.buffer.rfind(" ")
      else:
        break

      if cut <= 0:
        break

      text = self.buffer[:cut].strip()
      self.buffer = self.buffer[cut:]

      if text:
        self.text_queue.put(text)

  def _emit(self, event, data=None):
    self.events.put((event, data))

  def _synthesize(self):
    try:
      while True:
        kind, data = self.text_queue.get()

        if kind == "stop":
          break

        if kind == "flush":
          self.audio_queue.put(("flush", data))
          continue

        text = data

        self.audio_queue.put(("start", text))

        for chunk in self.model.generate_audio_stream(
          self.voice, text
        ):
          audio = (
            chunk.detach()
            .cpu()
            .numpy()
            .reshape(-1)
          )

          self.audio_queue.put(("audio", audio))

        self.audio_queue.put(("end", None))

    finally:
      self.audio_queue.put(("stop", None))

  def _play(self):
    try:
      with sd.OutputStream(
        samplerate=self.model.sample_rate,
        channels=1,
        dtype="float32",
        latency="low"
      ) as stream:

        while True:
          event, data = self.audio_queue.get()

          if event == "stop":
            break

          if event == "start":
            self._emit("start", data)

          elif event == "audio":
            audio = data

            for i in range(0, len(audio), 1024):
              block = audio[i:i + 1024]

              stream.write(
                block.reshape(-1, 1)
              )

              self._emit(
                "progress",
                len(block)
              )

          elif event == "end":
            self._emit("end")

          elif event == "flush":
            data.set()

    finally:
      self._emit("stop")

  def _finish(self):
    self.flush_and_wait()

    self.text_queue.put(("stop", None))

    self.tts_thread.join()
    self.audio_thread.join()
