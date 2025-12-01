## take in _raw.conllu files (generated previously, from xml, sentencised, tokenized…) and generate list of wikipedia thread ids from sentIDs
## use these thread IDs to filter the sendIDs to print
## return .conllu files, with onloy target threads, to send to retokeniser.

import pandas as pd
import numpy as np
import os, re, io, glob, conllu
from tqdm import tqdm

metadata_file = '/Users/Data/ANR_PREFAB/Data/Corpus/Wikis/metadata/EFG-FR-lexicoscope-metadata-2024-03-29.xlsx'
metadata = pd.read_excel(metadata_file)

target_ids = metadata['id'].values

def get_threadID_from_sentID(text):
  pattern = r"_[0-9]+?-[0-9]+?-\d+$"
  return re.sub(pattern, r"", text)
def import_parse_prefab_connl(input_file):
    """import and parse connlu file to list of lists
    Args:
    input_file: path to a connlu file
    Returns:
    parsed_data : data parsed from the connlu, as list of lists
    """
    with open(input_file, "r") as f:
      conllu_data = f.read()
  
    parsed_data = conllu.parse(conllu_data)
  
    return parsed_data

for g in ['E','F','G','H']:
  file_list = glob.glob(f'/Volumes/Data/ANR_PREFAB/F_talkPages/connlu/v6/{g}/{g}' + "*.conllu")
  for raw_data_file in tqdm(file_list):
    # K_raw_data_file = f'/Volumes/Data/ANR_PREFAB/F_talkPages/connlu/v6/{g}/{g}_00.conllu'
    L_raw_data  = import_parse_prefab_connl(raw_data_file)
    outputfule = raw_data_file.replace('_raw.conllu','.conllu')


    resultlist =[]
    for sentence in L_raw_data:
      sentID = sentence.metadata['sentID']
      thread_id = get_threadID_from_sentID(sentID)
      output = sentID, thread_id
      resultlist.append(output)
    
    keep_list = [resultlist[i] for i in range(len(resultlist)) if resultlist[i][1] in target_ids]
    # slow, try with np.where
    #len(keep_list)
    #len(resultlist)
    keeplist_sentIDs = np.array([keep_list[i][0] for i in range(len(keep_list))])
    
        
    keep_sents = [sentence for sentence in L_raw_data if sentence.metadata['sentID'] in keeplist_sentIDs]
    
    
    with io.open(outputfule, 'w', encoding='UTF-8') as f:
            for sentence in tqdm(keep_sents):
              if len(sentence)>0:
                meta_id = "# sentID = " + sentence.metadata['sentID']
                meta_text_raw = "# text_raw = " + sentence.metadata['text_raw'] 
                meta_text_prefab = "# text_prefab = " + sentence.metadata['text_prefab']
                metadata_chunk = "\n" + "\n".join([chunk for chunk in [meta_id, meta_text_raw, meta_text_prefab]]) +"\n"
                _ = f.write(metadata_chunk)
    
                for token in sentence:
                    my_string = str(token['id']) +"\t"+ str(token['form']) + "\t_\t_\t_\t_\t_\t_\t_\t_\n"
                    _ = f.write(my_string)

