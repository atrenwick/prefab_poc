# HOPS preparer
#imports
import platform, io, os, stanza, re, conllu, glob
from tqdm import tqdm
import pandas as pd

  
## functions
def build_rows_from_conll(conll_data):
    """
    take conllu data and for each sentence, return a tuple of the tokenID within the sentence, the token and a concatenation of the sentenceID and the tokenID to create a unique ID for each token.
    Args :
    	Input : 
    		conll_data : parsed conllu data to be prepared for processing with HOPS.
    	returns : 
    		all_sents : a list of tuples of (tokenID, token and UniqueID) for all sents in conllu data, including blank lines to separate sentences, to create input as HOPS expects, while allowing for conversion back to conllu.
    """
    all_sents = []
    for s in range(len(conll_data)):
        sent_rows = ["\t\t\n"]
        tokenlist = conll_data[s]
        for t, token in enumerate(tokenlist):
                index = str(t+1)
                mytoken = str(token)
                UUID = conll_data[s].metadata['sent_id'] +"-"+ str(t+1).zfill(3)
                row = index + "\t" + mytoken + "\t" + UUID + "\n"
                sent_rows.append(row)
        all_sents.append(sent_rows)        
    return all_sents

def export_all_sents_for_hops(all_sents, conll_file):
    """
    write all the strings in all_sents to a file
    Args :
    	input : all_sents: list of sentences with tuples of data for HOPS
    			conll_file : path to input conllu file, used to create path and filename to output_file.
    	Return : 
    			output_file : path to output file written
    			
    """
    output_file = conll_file.replace("conllu", "csv")
    with open(output_file, 'w', encoding = "UTF-8") as h:
        for sent in all_sents:
            for row in sent:
                h.write(row)
    #print(f'Exported to {output_file}')
    return output_file
    
def convert_conllu_for_hops(conll_file):
    """
    Convert a 10 column conllu file to a .csv file for HOPS
    Args : 
    	input : conll_file : absolute path to a conll_file with 10 columns to be parsed and prepared for HOPS
    	returns : output_file : absolute path to output_file written with data prepared for HOPS
    """
    conll_data = import_parse_prefab_connl(conll_file)
    all_sents = build_rows_from_conll(conll_data)
    output_file = export_all_sents_for_hops(all_sents, conll_file)
    return output_file

def import_parse_prefab_connl(input_file):
  """ helper function to import and parse connlu file 
  Args:
    input_file: path to a connlu file
  Returns:
    parsed_data : data parsed from the connlu, as list of lists
  """
  with open(input_file, "r", encoding='UTF-8') as f:
      conllu_data = f.read()
  
  parsed_data = conllu.parse(conllu_data)
  
  return parsed_data

def make_hops_command_df(local_path, remote_path, input_files):
    '''
    make a df of commands and export it to csv, to allow for processing of files in specific order.
    Args:
        local_path : header element of local path, to be replaced with remote path to create addresses on remote systems based on filenames on local system
        remote_path : header element of remote path, to be added in place of local_path, to create addresses on remote systems based on filenames of local system. Setting this to the same value as local_path to run in local-only mode
        input_files : list of absolute paths to files to send to HOPS. If input_files is hierarchically arranged with all the CSVs in a folder named 004, parsed files will be sent to folders existing at the same level, one folder per hops_model 
        platform : string, either w for windows or u for unix/linux, to control slashes in paths 
    Returns:
        Writes a csv file of HOPS commands in the same local folder as the first file in the input_files list.
    
    Special arguments defined in function:
        model_path : path to folder containing 1 or more HOPS models
        hops_models : names of HOPS models
        
    '''
    all_commands = [] # storage list
    model_path = '//Users/Data/HOPSmodels' # base path to model folder
    hops_models = [5262, 5272, 5282,  5287, 5292, 5297] # codes for models
    hops_head = 'hopsparser parse ' # pieces of HOPS command
    hops_tail = "  --device 'cuda'"
    ## note : devtest is for dev only, overriding the tail element of the hops command and forcing model_path to a specific value ; devtest is to be defined on run, not within a definition
    if devtest == True:
      model_path = '/Users/Data/HOPSmodelsv5/'
      hops_tail = ""
    folders_needed = [] # list of folders to create
    for hops_input_file in input_files:
        for hops_model in hops_models:
            hops_input_file = hops_input_file.replace('.conllu', '.csv') # replace file extensions
            tidy_inputfile = hops_input_file.replace(local_path, remote_path).replace('\\','/') # swap local for distant paths, replace windows slashes
            
            hops_output_full = tidy_inputfile.replace('.csv',f'-{str(hops_model)}.csv').replace('/004/',f'/{str(hops_model)}/') # replace 102.CSV with model code and 104 in the suffix
            dest_folder = os.path.dirname(hops_output_full) # get name of folder where output will be saved
            if os.path.exists(dest_folder) ==False:
                if dest_folder not in folders_needed:
                    folders_needed.append(dest_folder) # if the folder doesn't exist, add it to list
                    folder_command = f'mkdir {dest_folder}' # make command to make folder
                    all_commands.append(folder_command) # add folder-making command to list of all commands
            
            full_string = hops_head + model_path + '/' +str(hops_model) + " " + tidy_inputfile + " " + hops_output_full + hops_tail
        #print(full_string+ "\n")
            all_commands.append(full_string)
    df = pd.DataFrame(all_commands)
    return df


def prepare_for_HOPS(path, remote_path):
  '''
  Run preparations necessary for processing with HOPS : convert all files with ending 104.conllu to 104.csv formatted for HOPS, and then use these newly created files as input to make a list of commands to pass to a HOPS_runner function as a csv
  Args :
      local_path : absolute path to folder containing 104.conllu files which are to be converted to for HOPS
      remote_path : absolute path to folder on remote server where files will be placed for HOPS to look for them
  Returns : 
      df : a pd df of command line commands to run
  Notes :
    1. If running locally, set local_path and remote_path to the same value
    2. a special variable, devtest, can be set to true or false. If this is true, model_path, usually set to value for remote server, is set to local path with HOPS models. If true, this also removes the device specification argument from the command line argument
    
      
  '''
  
  local_path = path
  remote_path = remote_path
  input_files = glob.glob(path + "/*-104a*.conllu")
  files_internal_list = []
  devtest = True
  for i, input_file in enumerate(tqdm(input_files)):
      output_file = convert_conllu_for_hops(input_file)
      files_internal_list.append(output_file)
  
  input_files = files_internal_list
  df = make_hops_command_df(local_path, remote_path, input_files)
  hops_frame_path = re.sub("/$", "_hops_frame.csv", path)
  df.to_csv(hops_frame_path, sep="\t", index=False)
  return df  

## running
#for letter in ['Char']:
path =  f'/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3d1/Processing/RT/'
devtest= False
df = prepare_for_HOPS(path, "//Users/Data/PREFAB/wiki_sents_to_reprocess/")

