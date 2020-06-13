import time
import psutil

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument("-l", "--label", dest="label",
                    help="label the csv", default="")
parser.add_argument("-t", "--time", type=int,
                    dest="time", default=120,
                    help="run the scripts in how many seconds")

args = parser.parse_args()

print("Writing to \"" + args.label + "load.csv\"")
print("Will run " + str(args.time) + " seconds")

with open(args.label+"load.csv", "w") as load_file:
  t_end = time.time() + args.time 
  while time.time() < t_end:
    load_file.write(f"{psutil.cpu_percent()},{psutil.virtual_memory().percent}\n")
    load_file.flush()
    time.sleep(1)