

import subprocess

result = subprocess.run("find . -type f", shell=True, capture_output=True, text=True)
print(result.stdout)