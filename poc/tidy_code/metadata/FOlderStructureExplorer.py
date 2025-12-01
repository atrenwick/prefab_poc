## py version of dump inventory maker


import os
import pandas as pd

folder_structure = pd.DataFrame(glob.glob(folder_path, recursive=True))
 
def print_structure_here(folder_path, save_full):
 ''' function to take folder and produce recursive list of contents as txt file in specified location.
   Saves file as text and returns pd dataframe of filestructure as folder_structure
  args:
   folder_path = path to folder of which structure will be printed. Variable folder_path_full created with wildcards to force full recursiveness
   save_full = full path + name+ extension of file to be printed
  
 '''
 import os
 import pandas as pd
 folder_path_full = folder_path + "**/**"
 folder_structure = pd.DataFrame(glob.glob(folder_path_full, recursive=True))

 with open(save_full, 'w') as f:
  for i in range(len(folder_structure)):
   f.write(folder_structure.iloc[i,0]+ "\n")
 return folder_structure


ANR_folder = "cefc-orfeo"
folder_path = "/Users/Data/ANR_PREFAB/" + ANR_folder
save_full = "/Users/Data/" +ANR_folder + "_structure.txt"

folder_structure = print_structure_here(folder_path, save_full)



orfeo_tcof = pd.DataFrame(glob.glob("/Users/Data/ANR_PREFAB//cefc-orfeo/11/oral/tcof/**", recursive=True))
plain_tcof = pd.DataFrame(glob.glob("/Users/Data/ANR_PREFAB/tcof/**", recursive=True))



extensions = ".orfeo", ".wav", ".mp4", ".mp3", ".m4a", '.xml', ".trs",".orf", "xsl"
i=23
a for a in a if a
e=2
all_list = list()
for e in range(len(extensions)):
 current_list= [i for i in range(len(plain_tcof)) if extensions[e] in plain_tcof.iloc[i,0]]
 all_list = all_list + current_list

plain_tcof_subset = plain_tcof[i, 0]
plain_tcof.iloc[1037, 0]
plain_tcof.columns = ["file_path_and_file_name"]
outputdf['colname'] = pd.DataFrame([ plain_tcof.iloc[i,0] if (workingFrame.iloc[b, 6] == 0) else workingFrame.iloc[b,1] for b in range(len(workingFrame)) ]) 


# Define the list of file extensions
extensions = [".orfeo", ".wav", ".mp4", ".mp3", ".m4a", '.xml', ".trs",".orf", "xsl"]

def separate_file_path(df):
 """Separates the file paths from the name of the files and adds a column for the file extensions if they are on the list of provided extensions.
 Args: df: A Pandas DataFrame with a column containing the file paths and file names.
 Returns:   A Pandas DataFrame with a column containing the file paths, a column containing the file names, and a column containing the file extensions.
 """
 # Create empty lists to store the file paths, file names, and file extensions
 file_paths = []
 file_names = []
 file_extensions = []

 # Iterate over the rows in the DataFrame
 for row in df.itertuples():
  # Get the file path and file name from the row
  file_path_and_file_name = row.file_path_and_file_name

  # Split the file path and file name into separate parts
  file_path, file_name = file_path_and_file_name.rsplit(os.sep, 1)

  # Check if the file path is a folder
  if os.path.isdir(file_path):
   # Ignore folders
   continue

  # Get the file extension
  file_extension = file_name[-4:]

  # Add the file path, file name, and file extension to the corresponding lists
  file_paths.append(file_path)
  file_names.append(file_name)
  file_extensions.append(file_extension)

 # Create a new DataFrame with the file paths, file names, and file extensions
 df_new = pd.DataFrame({
  'file_path': file_paths,
  'file_name': file_names,
  'file_extension': file_extensions
 })

 # Filter the file extensions to only include the extensions in the provided list
 df_new['file_extension'] = df_new['file_extension'].where(df_new['file_extension'].isin(extensions))

 # Return the DataFrame
 return df_new

# Separate the file paths from the name of the files and add a column for the file extensions
df = separate_file_path(plain_tcof)

source_file = "/Users/Data/prédiction/TCOF/39_45_eva_14/39_45_eva_14 copy.tsv"



rawConll = open(source_file, 'rt', encoding="UTF8")
intConll = rawConll.read()
import conllu
parsed_conllu = conllu.StringIO(intConll)

col_names = ['ID', 'FORM', 'LEMMA', 'XPOS' ,'UPOS','FEATS', 'HEAD' ,'DEPREL', 'NONE1', 'NONE2', 'TIME1', 'TIME2' ,'SPEAKER' ,'CHANGELOC', 'PAUSE', 'LEX', 'SUP']


input_raw = pd.read_csv(source_file, sep="\t", skip_blank_lines=False, na_filter=True, encoding="UTF8", low_memory=False, names = col_names)
subset = input_raw[['ID','FORM']]
subset.to_csv("/Users/Data/test.csv", encoding="UTF8", index=False)
