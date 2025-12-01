# conll splitter
# helper to split large conllu files into smaller files, with aa,ab,ac… suffixes added to facilitate sending to Stanza, UDPipe

from itertools import islice
from stanza.utils.conll import CoNLL
import io, os, conllu, glob
from string import ascii_lowercase
from math import ceil
from tqdm import tqdm

# 
def import_parse_prefab_connl(input_file):
 """import and parse connlu file to list of lists
 Args:
  input_file: path to a connlu file
 Returns:
  parsed_data : data parsed from the connlu, as list of lists
 """
 # read in data, parse as conllu
 with open(input_file, "r", encoding='UTF-8') as f:
   conllu_data = f.read()
 # tidy apostrophes, send to parsed_data
 conllu_data = conllu_data.replace("’","'").replace("'","'").replace("’","'")
 parsed_data = conllu.parse(conllu_data)

 return parsed_data


def generate_two_letter_suffixes(num_suffixes):
 """
 Generates a list of unique sequential 2-letter suffixes (aa to zz), sufficient for 676 slices of input folder.

 Args:
   num_suffixes: The number of unique suffixes needed.

 Returns:
   A list of strings containing the generated 2-letter suffixes, or ValueError if more tha 676 slices needed.
 """
 if num_suffixes > 26 * 26:
  raise ValueError("Number of suffixes exceeds maximum (26 * 26)")

 suffixes = []
 for i in range(26):
  for j in range(26):
   # Convert characters to lowercase letters (a-z)
   first_letter = chr(ord('a') + i)
   second_letter = chr(ord('a') + j)
   suffixes.append(first_letter + second_letter)

   # Stop generating suffixes if enough are created
   if len(suffixes) == num_suffixes:
    return suffixes

 return suffixes


def split_conllu(parsed_data, step_size):
  """Define the contents and name of each slice the conllu data will be divided into based on a given size for each chunk, defined as a number of sentences.

   Args:
   parsed_data: conllu data imported and parsed, to be split into smaller chunks.

  Returns:
   segments : A list of lists, where each sublist contains a chunk of the original data.
   segment_names : a list of suffixes to add to filenames
  """
  # define step size of n sentences to stay under 4,4 MB limit for ROMANS ; for aliged phraseorom_fr-de, can use 3000
  step_size = int(step_size)
  # calculate number of steps needed, using ceil to round up
  step_count = ceil(len(parsed_data)/step_size)
  # generate lists of sentence ranges for each chunk
  segments = [list(islice(parsed_data, i * step_size, (i + 1) * step_size)) for i in range(step_count)]
  # get lowercase ascii letter for each segment as list 
  segment_names = generate_two_letter_suffixes(len(segments))
  return segments, segment_names




# need to then take lists and loop over them
def distribute_conll_strings(segments):
  """
  Take the lists of contents + names for slices, extract sentence-level metadata and prepare conllu data as strings for export
  
  """
  # create list to hold all outputs of all files
  segment_store = []
  # iterate over segments = 1 segment for each file
  for segment in segments:
    # reset the string_store = list of strings for each file; rename segment
    string_store = []
    target_list = segment
    # iterate over all the sentences
    for i in range(len(target_list)):
     # get the current sentence and get sent level metas
     current_sentence = target_list[i]
     id_string = str(current_sentence.metadata['sentID'])
     meta_text_raw = '# meta_text_raw = ' + str(current_sentence.metadata['text_raw'])
     meta_text_prefab = str(current_sentence.metadata['text_prefab'])
     metas = str("\n# sent_id = ") + str(id_string) + '\n'  + str(meta_text_raw) + "\n" + str("# meta_text_prefab = ") + str(meta_text_prefab ) + "\n"
     # send all sent-level metas to string_store, as a string
     string_store.append(metas)
     # for each token in the current sentence, if misc col is None, set PLX to _, otherwise set it to the 0th value of the dict recast as a list
     for m in current_sentence:
      if str(m['misc']) == 'None':
       PLX = "_" 
      else:
       PLX = list(m['misc'])[0]
      # concat strings to build a line, send to string_store
      outputstring = str(m['id']) + '\t' + str(m['form'])+ '\t_\t_\t_\t_\t_\t_\t_\t' + PLX + '\n'
      string_store.append(outputstring)
          #f.write(outputstring)
    # after processing all the sentences for a file, add string_store to segment_store, then go to next file    
    segment_store.append(string_store)
  return segment_store

def export_to_sep_conll_files(segment_store, segment_names, input_file):
 '''
 iterate over segment store and segment names to export data to outputfile with name based on segment name and input file
 args
   : inputs
     : segment_store : list of lists of strings, one list per file to export, each sublist containing all the strings for the document
     : segment_names : sequential letters used to provide unique sequential names of files
     : input_file : full path to input file
   : returns :
     no return object :: exports a conllu file as output
 '''
 # get path to directory containing file being analysed
 dir_path = os.path.dirname(input_file)
 # remove path and suffix, extension to yield code of text
 text_code = input_file.replace(dir_path,'').replace("/","").replace(".conllu","")
 # LC to add path, text code, segment code and extension for each file to be produced
 filenames = [ (str(dir_path) + "/" + str(text_code) + str(segment_names[i]) + '.conllu') for i in range(len(segment_names))]
 # LC to get string lists qs list of same length as filenames
 string_lists = [string_list for string_list in segment_store]
 # iterate over filenames, strings; zipping them, opening file connection and writing strings
 for filename, string_list in zip(filenames, string_lists):
  with open (filename, 'w', encoding="UTF-8") as f:
   for string in string_list:
    f.write(string)
 return filenames

## run
#### recommended values for step_size in split_conllu function to remain under UDPipe limit of 4,3 MB:
########### WIKI : use n = 4000 sentences
########### Romans : n = 3000 sentences
########### Oral : n = 3000 sentences

## current_setup: for letter in list of letters, each being its own subfolder within the v7 folder of the wiki corpus:
#for letter in tqdm(['C','D','E','F','I','J','K','L','M','N','O','P','Q']):
for letter in tqdm(['RT']):
  input_files = glob.glob(f"/Users/Data/ANR_PREFAB/CorpusPREFAB/WikiDiscussions/WikiDiscussions_V3d1/Processing/{letter}/" + f"*-104.conllu")
  #input_files = glob.glob(f"/Users/Data/ANR_PREFAB/F_talkPages/connlu_wiki/v7/{letter}/" + f"*-104.conllu")
  for input_file in input_files:
      parsed_data = import_parse_prefab_connl(input_file)
      segments, segment_names = split_conllu(parsed_data, 3000)
      segment_store= distribute_conll_strings(segments)
      export_to_sep_conll_files(segment_store, segment_names, input_file)
      
      
