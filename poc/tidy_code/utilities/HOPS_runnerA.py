# py script to take a hard-coded input and use it to load a pd df of commands to execute
from tqdm import tqdm
import os
import pandas as pd
input_file = '//Users/Data/PREFAB/wiki_final/hopscommandsA.csv'
df = pd.read_csv(input_file, sep='\t', header=None)
for i in tqdm(range(len(df))):
	command = df.iloc[i, 0]
	os.system(command)
	
## TO DO : reconfigure to run as CLI