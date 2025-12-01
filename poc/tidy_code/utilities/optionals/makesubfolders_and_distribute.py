# helper loop to take a list of letters and create folders for each letter and move files into each folder, for the WikiCorpus which necessarily contains many folders
## running this with chunks rather than letters allows for codes for romans to be used
import os, glob
import pandas as pd
import time
from tqdm import tqdm
## make folder
# start = time.time()
for letter in tqdm(['A','B','C','D','E','F','G','H','I','J','K','L','M', 'N','O','P','Q','R','S','T','U','V','W','X','Y','Z']):
  # letter ="A"
  gen_folder = f'//Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/{letter}/'
  subfolders = glob.glob(gen_folder + '*-104.conllu')
  subfoldernames = [folder.replace(gen_folder,'').replace('-104.conllu','') for folder in subfolders]
  for name in subfoldernames:
          newdir = gen_folder + name + "/"
          if os.path.exists(newdir) is False:
              os.makedirs(newdir)
  
          these_files = glob.glob(gen_folder + name  + "*.*")
          for file in these_files:
              oldname = file
              newname = file.replace(gen_folder, newdir)
              os.rename(oldname, newname)
# end = time.time()    
