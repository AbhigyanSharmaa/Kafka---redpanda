# is_venv_active.py
import sys
import os

print("Virtual Env Active? ", sys.prefix != sys.base_prefix)
print("Current Prefix: ", sys.prefix)
print("Base Prefix: ", sys.base_prefix)
print("VIRTUAL_ENV:", os.getenv("VIRTUAL_ENV"))