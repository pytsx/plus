
from pathlib import Path

from pydantic import BaseModel

from function import Function

class ReadFileProps(BaseModel):
  filepath: str

def _read_file(props: ReadFileProps) -> str | None:
  path = Path(props.filepath)
  if not path.is_file():
    print("Error: File not found.")
    return None
  return path.read_text()

read_file = Function(
  name="read_file",
  description="Reads a file and returns its content.",
  execute=_read_file,
  args_schema=ReadFileProps
)
