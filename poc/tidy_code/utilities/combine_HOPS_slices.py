## join HOPS slices as CSV

import platform, io, os, stanza, re, glob
from stanza.utils.conll import CoNLL
from tqdm import tqdm


# path to folder in which to look for files to process
path = '/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3d1/Processing/'
# get names of subfolders, with end slash, then remove path
chunks = glob.glob(path + "/*/")
chunks = [chunk.replace(path, '') for chunk in chunks]
 


# loop to take specific folders of input and for each folder, iterate over model_codes of (imitation) HOPS data and concatenate true HOPS annotations with imitation HOPS annotations, and export a single file per parser
# can define manually as below, or from path as above
#chunks = ["B_01", "C_04", "F_00", "", "M_03", "P_01"]

for chunk_code in chunks:
 print(chunk_code)
 # iterate over all 6 models
 for model_code in ['5262', '5272', '5282', '5287', '5292', '5297']:
  ## reset csv data at for each model code, and make name for output file 
  csv_data = ""
  output_file = f'{path}{chunk_code}-{model_code}.csv'
 	## get the sorted list of files from the path and the chunk_code subfolder, and all files with the model code.csv as ending
  csv_files = sorted(glob.glob(f'{path}{chunk_code}/*{model_code}.csv'))
  # enumerate over the files, reading in, coercing to string, adding linebreak, and data read in
  for f, file in tqdm(enumerate(csv_files)):
   with open(file, 'r', encoding = "UTF-8") as c:
    csv_in = c.read()
    csv_data = str(csv_data) + "\n" + csv_in
  # when all files have been read in, write the output to a single file  
  with open(output_file, 'w', encoding = "UTF-8") as o:
   _ = o.write(csv_data)
  
