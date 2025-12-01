import os, glob, time
import pandas as pd
from tqdm import tqdm

def runcommand(currentCommand):
  """
  ultra simple function to take a command and run it with os.system
  Args : 
  	input : currentCommand : a command to send to os.system
  	return : no return object ; commands will create a filebased on UDPipe output
  """
  os.system(currentCommand)

# load the target df of udpipe commands
myfile = "/data/PREFAB/udpipe_commands.csv"
allResults =  pd.read_csv(myfile, sep="\t", header=None)

# iterate over df executing commands, pausing for 20s then repeat loop. this way we don't hit the server hard.
for i in tqdm(range(0, len(allResults))):
#for i in range(50,52): 
  currentCommand = allResults.iloc[i,0]
  runcommand(currentCommand)
  #print("Got " + str(int(i+1)) +  " of " + str(len(allResults)))
  time.sleep(20)