import time
import psutil

with open("load.csv", "w") as load_file:
  while True:
    load_file.write(f"{psutil.cpu_percent()},{psutil.virtual_memory().percent}\n")
    load_file.flush()
    time.sleep(1)